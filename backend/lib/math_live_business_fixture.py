"""Strict zero-model validation for frozen live math business captures.

The fixture root is treated as immutable input.  The validator may read the
declared Obsidian source cards and Codex rollout used for provenance, but it
never writes into the fixture, invokes MCP, submits a model request, or calls a
writer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping, Sequence


BATCH_SCHEMA = "math-luna-business-task-batch-v1"
REAL_AUTHORITY_SCHEMA = "math-luna-real-business-samples-v1"
ITEM_SCHEMA = "math-luna-business-task-manifest-v1"
RECORD_SCHEMA = "math-luna-business-capture-v1"
DIALOGUE_SCHEMA = "math-verbatim-dialogue-index-v1"
RESULT_SCHEMA = "study-intake-math-live-business-fixture-validation-v1"
FINGERPRINT_ALGORITHM = (
    "sha256_of_sorted_relative_path_and_file_sha256_lines_excluding_manifest"
)
SOLUTION_REQUIREMENT = "solution_text_or_solution_image"
MODEL_REQUEST = {
    "model": "gpt-5.6-luna",
    "reasoning_effort": "max",
    "runtime_attestation": "requested_unverified",
}
DEFAULT_TRUSTED_SOURCE_ROOT = Path("/Users/xiazhibin/Documents/kaoyan-math")
DEFAULT_TRUSTED_ROLLOUT_ROOT = Path("/Users/xiazhibin/.codex/sessions")
REAL_AUTHORITY_CAPTURE_IDS = {
    "LUNA-MATH-20260809-001",
    "LUNA-MATH-20260809-002",
    "LUNA-MATH-20260809-003",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
ISO_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)

TASK_RULES: dict[str, dict[str, Any]] = {
    "independent_correct_no_false_wrong_card": {
        "roles": {
            "question_image",
            "solution_evidence",
            "verbatim_user_answer",
            "verbatim_assistant_feedback",
            "dialogue_sequence_and_provenance",
            "business_task_record",
        },
        "dialogue_files": [
            "user_first_answer.md",
            "assistant_feedback.md",
            "assistant_feedback.md",
            "assistant_feedback.md",
        ],
        "dialogue_speakers": ["user", "assistant", "assistant", "assistant"],
        "reasoning_status": "user_attested_process_correct_but_did_not_transcribe_steps",
        "reasoning_boundary": "unobserved_steps_remain_unknown",
    },
    "wrong_then_corrected_weakness_proposal": {
        "roles": {
            "question_image",
            "solution_evidence",
            "verbatim_initial_user_reasoning",
            "verbatim_user_objection",
            "verbatim_partial_self_correction",
            "verbatim_second_self_correction",
            "verbatim_transfer_and_capture_request",
            "verbatim_assistant_feedback",
            "dialogue_sequence_and_provenance",
            "business_task_record",
        },
        "dialogue_files": [
            "user_reasoning.md",
            "assistant_feedback.md",
            "user_objection.md",
            "assistant_feedback.md",
            "user_followup.md",
            "assistant_feedback.md",
            "user_self_correction.md",
            "assistant_feedback.md",
            "user_resolution.md",
            "assistant_feedback.md",
        ],
        "dialogue_speakers": [
            "user",
            "assistant",
            "user",
            "assistant",
            "user",
            "assistant",
            "user",
            "assistant",
            "user",
            "assistant",
        ],
        "reasoning_status": "verbatim_user_reasoning_and_corrections_present",
        "reasoning_boundary": "independent_and_post_explanation_steps_are_separate",
    },
    "wrong_then_corrected_multistage_method_proposal": {
        "roles": {
            "question_image",
            "solution_evidence",
            "verbatim_user_volume_reasoning",
            "verbatim_user_riemann_reasoning",
            "verbatim_user_resolution",
            "verbatim_assistant_feedback",
            "dialogue_sequence_and_provenance",
            "business_task_record",
        },
        "dialogue_files": [
            "user_reasoning_part1.md",
            "assistant_feedback.md",
            "user_reasoning_part2.md",
            "assistant_feedback.md",
            "user_final_confirmation.md",
        ],
        "dialogue_speakers": ["user", "assistant", "user", "assistant", "user"],
        "reasoning_status": "verbatim_user_reasoning_and_assistant_feedback_present",
        "reasoning_boundary": "new_source_multistage_breaks_remain_separate",
    },
}

ROLE_VISIBILITY = {
    "question_image": "answer_safe_question_surface",
    "solution_text": "private_evidence",
    "solution_image": "private_evidence",
    "verbatim_user_answer": "private_evidence",
    "verbatim_initial_user_reasoning": "private_evidence",
    "verbatim_user_objection": "private_evidence",
    "verbatim_partial_self_correction": "private_evidence",
    "verbatim_second_self_correction": "private_evidence",
    "verbatim_transfer_and_capture_request": "private_evidence",
    "verbatim_user_volume_reasoning": "private_evidence",
    "verbatim_user_riemann_reasoning": "private_evidence",
    "verbatim_user_resolution": "private_evidence",
    "verbatim_assistant_feedback": "private_evidence",
    "dialogue_sequence_and_provenance": "private_evidence",
    "business_task_record": "private_evidence",
}


class MathLiveBusinessFixtureError(RuntimeError):
    """A stable fail-closed error code for an invalid business fixture."""


def _fail(code: str) -> None:
    raise MathLiveBusinessFixtureError(code)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(
    path: Path,
    code: str,
    *,
    max_bytes: int = 4 * 1024 * 1024,
) -> dict[str, Any]:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or not (0 < node.st_size <= max_bytes):
            raise OSError("unsafe input")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MathLiveBusinessFixtureError(code) from exc
    if not isinstance(value, dict):
        _fail(code)
    return value


def _expect_keys(value: Mapping[str, Any], expected: set[str], code: str) -> None:
    if set(value) != expected:
        _fail(code)


def _inside(root: Path, value: Path, code: str) -> Path:
    try:
        resolved = value.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise MathLiveBusinessFixtureError(code) from exc
    return resolved


def _safe_relative(value: Any, code: str) -> str:
    if not isinstance(value, str) or not value:
        _fail(code)
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts or str(relative) != value:
        _fail(code)
    return value


def _safe_id(value: Any, code: str) -> str:
    if not isinstance(value, str) or SAFE_ID_RE.fullmatch(value) is None:
        _fail(code)
    return value


def _valid_sha(value: Any, code: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        _fail(code)
    return value


def _authorization(value: Any, *, include_quick: bool, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail(code)
    expected = {
        "processing_authorized",
        "luna_authorized",
        "sol_authorized",
        "formal_write_count",
    }
    if include_quick:
        expected |= {"quick_intake_written", "scope"}
    _expect_keys(value, expected, code)
    if (
        value.get("processing_authorized") is not True
        or value.get("luna_authorized") is not True
        or value.get("sol_authorized") is not False
        or value.get("formal_write_count") != 0
        or (include_quick and value.get("quick_intake_written") is not False)
        or (
            include_quick
            and value.get("scope") != "exactly_the_two_listed_business_task_ids"
        )
    ):
        _fail(code)
    return dict(value)


def _model_request(value: Any, code: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or dict(value) != MODEL_REQUEST:
        _fail(code)
    return dict(MODEL_REQUEST)


def _runtime_authority(value: Any, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail(code)
    luna = value.get("luna")
    sol = value.get("sol")
    quick = value.get("quick_intake")
    if (
        not isinstance(luna, Mapping)
        or luna.get("processing_authorized") is not True
        or luna.get("local_database_read_authorized") is not True
        or luna.get("output_role") != "preprocessing_proposal"
        or luna.get("formal_library_write_authorized") is not False
        or not isinstance(sol, Mapping)
        or sol.get("formal_library_writer") is not True
        or sol.get("execution_authorization")
        != "governed_by_downstream_formal_workflow"
        or not isinstance(quick, Mapping)
        or quick.get("authorization") != "governed_by_live_intake_system"
        or value.get("current_formal_write_count", 0) != 0
    ):
        _fail(code)
    return {
        "luna_processing_authorized": True,
        "luna_local_database_read_authorized": True,
        "luna_output_role": "preprocessing_proposal",
        "luna_formal_library_write_authorized": False,
        "sol_formal_library_writer": True,
        "sol_execution_authorization": "governed_by_downstream_formal_workflow",
        "quick_intake_authorization": "governed_by_live_intake_system",
        "formal_write_count": 0,
    }


def _snapshot(root: Path) -> dict[str, dict[str, Any]]:
    try:
        node = root.lstat()
    except OSError as exc:
        raise MathLiveBusinessFixtureError("math_live_fixture_root_invalid") from exc
    if root.is_symlink() or not root.is_dir():
        _fail("math_live_fixture_root_invalid")
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        try:
            entry = path.lstat()
        except OSError as exc:
            raise MathLiveBusinessFixtureError("math_live_fixture_tree_invalid") from exc
        if path.is_symlink():
            _fail("math_live_fixture_symlink_forbidden")
        if path.is_dir():
            result[relative] = {"kind": "directory", "mode": entry.st_mode & 0o7777}
        elif path.is_file():
            result[relative] = {
                "kind": "file",
                "mode": entry.st_mode & 0o7777,
                "size": entry.st_size,
                "sha256": _sha256(path),
            }
        else:
            _fail("math_live_fixture_special_file_forbidden")
    return result


def _scoped_snapshot(
    root: Path,
    *,
    manifest_path: Path,
    capture_ids: set[str],
) -> dict[str, dict[str, Any]]:
    """Hash only the frozen batch surface, never sibling capture contents."""

    result: dict[str, dict[str, Any]] = {}

    def add(path: Path) -> None:
        relative = str(path.relative_to(root))
        try:
            entry = path.lstat()
        except OSError as exc:
            raise MathLiveBusinessFixtureError("math_live_fixture_tree_invalid") from exc
        if path.is_symlink():
            _fail("math_live_fixture_symlink_forbidden")
        if path.is_dir():
            result[relative] = {"kind": "directory", "mode": entry.st_mode & 0o7777}
        elif path.is_file():
            result[relative] = {
                "kind": "file",
                "mode": entry.st_mode & 0o7777,
                "size": entry.st_size,
                "sha256": _sha256(path),
            }
        else:
            _fail("math_live_fixture_special_file_forbidden")

    add(root / "contract.json")
    add(manifest_path)
    add(root / "items")
    for capture_id in sorted(capture_ids):
        item_dir = root / "items" / capture_id
        add(item_dir)
        for path in sorted(item_dir.rglob("*")):
            add(path)
    return result


def _authority_snapshot(
    root: Path,
    *,
    authority_files: Sequence[Path],
    capture_ids: set[str],
) -> dict[str, dict[str, Any]]:
    """Hash the complete declared authority surface without following links."""

    result: dict[str, dict[str, Any]] = {}

    def add(path: Path) -> None:
        try:
            relative = str(path.relative_to(root))
            node = path.lstat()
        except (OSError, ValueError) as exc:
            raise MathLiveBusinessFixtureError(
                "math_live_fixture_tree_invalid"
            ) from exc
        if path.is_symlink():
            _fail("math_live_fixture_symlink_forbidden")
        if path.is_dir():
            result[relative] = {"kind": "directory", "mode": node.st_mode & 0o7777}
        elif path.is_file():
            result[relative] = {
                "kind": "file",
                "mode": node.st_mode & 0o7777,
                "size": node.st_size,
                "sha256": _sha256(path),
            }
        else:
            _fail("math_live_fixture_special_file_forbidden")

    for path in sorted(set(authority_files)):
        add(path)
    items_root = root / "items"
    add(items_root)
    for capture_id in sorted(capture_ids):
        item_dir = items_root / capture_id
        add(item_dir)
        for path in sorted(item_dir.rglob("*")):
            add(path)
    return result


def _item_directory_scope(
    items_root: Path,
    *,
    listed_capture_ids: set[str],
) -> tuple[set[str], list[str]]:
    actual: set[str] = set()
    for path in items_root.iterdir():
        try:
            path.lstat()
        except OSError as exc:
            raise MathLiveBusinessFixtureError(
                "math_live_fixture_item_directory_invalid"
            ) from exc
        # Only the sibling entry itself is inspected.  Contents of an
        # out-of-scope capture belong to a later batch and are never traversed.
        if path.is_symlink() or not path.is_dir():
            _fail("math_live_fixture_item_directory_invalid")
        actual.add(path.name)
    missing = listed_capture_ids - actual
    if missing:
        _fail("math_live_fixture_listed_item_directory_missing")
    return actual, sorted(actual - listed_capture_ids)


def _snapshot_sha(snapshot: Mapping[str, Any]) -> str:
    raw = json.dumps(
        snapshot,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _fingerprint(rows: Sequence[tuple[str, str]]) -> str:
    body = "".join(f"{digest}  ./{relative}\n" for relative, digest in sorted(rows))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _read_nonempty_text(path: Path, code: str, *, max_bytes: int = 2 * 1024 * 1024) -> str:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or not (0 < node.st_size <= max_bytes):
            raise OSError("unsafe text")
        value = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise MathLiveBusinessFixtureError(code) from exc
    if not value.strip():
        _fail(code)
    return value


def _verify_png(path: Path, code: str) -> None:
    try:
        node = path.lstat()
        signature = path.read_bytes()[:8]
    except OSError as exc:
        raise MathLiveBusinessFixtureError(code) from exc
    if (
        path.is_symlink()
        or not path.is_file()
        or not (8 < node.st_size <= 32 * 1024 * 1024)
        or path.suffix.lower() != ".png"
        or signature != b"\x89PNG\r\n\x1a\n"
    ):
        _fail(code)


def _frontmatter(path: Path) -> tuple[dict[str, str], str]:
    raw = _read_nonempty_text(path, "math_live_solution_text_invalid")
    if not raw.startswith("---\n") or "\n---\n" not in raw[4:]:
        _fail("math_live_solution_frontmatter_invalid")
    header, body = raw[4:].split("\n---\n", 1)
    values: dict[str, str] = {}
    for line in header.splitlines():
        if not line.strip() or ":" not in line:
            _fail("math_live_solution_frontmatter_invalid")
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value or key in values:
            _fail("math_live_solution_frontmatter_invalid")
        values[key] = value
    if not body.strip():
        _fail("math_live_solution_text_empty")
    return values, body


def _message_text(payload: Mapping[str, Any]) -> str:
    content = payload.get("content")
    if not isinstance(content, list):
        return ""
    chunks: list[str] = []
    for item in content:
        if (
            isinstance(item, Mapping)
            and item.get("type") in {"input_text", "output_text"}
            and isinstance(item.get("text"), str)
        ):
            chunks.append(str(item["text"]))
    return "\n".join(chunks).strip()


def _rollout_messages(
    path: Path,
    wanted: set[tuple[str, str]],
    *,
    trusted_rollout_root: Path,
) -> tuple[dict[tuple[str, str], str], str]:
    if not path.is_absolute():
        _fail("math_live_rollout_path_invalid")
    if path.is_symlink():
        _fail("math_live_rollout_path_invalid")
    source = _inside(trusted_rollout_root, path, "math_live_rollout_path_invalid")
    try:
        node = source.lstat()
    except OSError as exc:
        raise MathLiveBusinessFixtureError("math_live_rollout_invalid") from exc
    if source.is_symlink() or not source.is_file() or not (0 < node.st_size <= 1024**3):
        _fail("math_live_rollout_invalid")
    found: dict[tuple[str, str], str] = {}
    try:
        with source.open("rb") as handle:
            for raw_line in handle:
                try:
                    row = json.loads(raw_line.decode("utf-8"))
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise MathLiveBusinessFixtureError("math_live_rollout_jsonl_invalid") from exc
                if not isinstance(row, Mapping):
                    continue
                payload = row.get("payload")
                if not isinstance(payload, Mapping) or payload.get("type") != "message":
                    continue
                timestamp = row.get("timestamp")
                role = payload.get("role")
                key = (str(timestamp), str(role))
                if key not in wanted:
                    continue
                value = _message_text(payload)
                if not value or key in found:
                    _fail("math_live_rollout_turn_ambiguous")
                found[key] = value
    except OSError as exc:
        raise MathLiveBusinessFixtureError("math_live_rollout_invalid") from exc
    if set(found) != wanted:
        _fail("math_live_rollout_turn_missing")
    selected_turns = [
        {"timestamp": timestamp, "speaker": speaker, "text": found[(timestamp, speaker)]}
        for timestamp, speaker in sorted(found)
    ]
    selected_sha = hashlib.sha256(
        json.dumps(
            selected_turns,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return found, selected_sha


def _assistant_section(raw: str, timestamp: str) -> str:
    marker = f"## {timestamp}"
    if raw.count(marker) != 1:
        _fail("math_live_assistant_section_invalid")
    section = raw.split(marker, 1)[1]
    if "\n## " in section:
        section = section.split("\n## ", 1)[0]
    if not section.strip():
        _fail("math_live_assistant_section_invalid")
    return section.strip()


def _validate_dialogue(
    path: Path,
    *,
    item_dir: Path,
    declared_paths: set[str],
    task_kind: str,
    trusted_rollout_root: Path,
) -> dict[str, Any]:
    value = _load_json(path, "math_live_dialogue_index_invalid")
    _expect_keys(
        value,
        {
            "schema_version",
            "session_id",
            "source_rollout",
            "turns",
            "reasoning_transcript_status",
            "fabricated_turn_count",
        },
        "math_live_dialogue_index_keys_invalid",
    )
    rule = TASK_RULES[task_kind]
    turns = value.get("turns")
    if (
        value.get("schema_version") != DIALOGUE_SCHEMA
        or _safe_id(value.get("session_id"), "math_live_dialogue_session_invalid")
        is None
        or value.get("reasoning_transcript_status") != rule["reasoning_status"]
        or value.get("fabricated_turn_count") != 0
        or not isinstance(turns, list)
        or len(turns) != len(rule["dialogue_files"])
    ):
        _fail("math_live_dialogue_contract_invalid")

    wanted: set[tuple[str, str]] = set()
    validated: list[tuple[dict[str, Any], Path]] = []
    previous_timestamp = ""
    for index, turn in enumerate(turns, start=1):
        if not isinstance(turn, Mapping):
            _fail("math_live_dialogue_turn_invalid")
        speaker = turn.get("speaker")
        expected_keys = {"sequence", "timestamp", "speaker", "file"}
        expected_keys.add("evidence_kind" if speaker == "user" else "section")
        _expect_keys(turn, expected_keys, "math_live_dialogue_turn_keys_invalid")
        timestamp = turn.get("timestamp")
        relative = _safe_relative(turn.get("file"), "math_live_dialogue_file_invalid")
        if (
            turn.get("sequence") != index
            or speaker != rule["dialogue_speakers"][index - 1]
            or relative != rule["dialogue_files"][index - 1]
            or not isinstance(timestamp, str)
            or ISO_TIMESTAMP_RE.fullmatch(timestamp) is None
            or (previous_timestamp and timestamp <= previous_timestamp)
            or relative not in declared_paths
        ):
            _fail("math_live_dialogue_turn_invalid")
        if speaker == "user":
            if not isinstance(turn.get("evidence_kind"), str) or not turn["evidence_kind"]:
                _fail("math_live_dialogue_turn_invalid")
        elif turn.get("section") != timestamp:
            _fail("math_live_dialogue_turn_invalid")
        artifact = _inside(item_dir, item_dir / relative, "math_live_dialogue_file_invalid")
        wanted.add((timestamp, str(speaker)))
        validated.append((dict(turn), artifact))
        previous_timestamp = timestamp

    rollout_path = Path(str(value.get("source_rollout") or ""))
    messages, selected_turns_sha = _rollout_messages(
        rollout_path,
        wanted,
        trusted_rollout_root=trusted_rollout_root,
    )
    exact = 0
    subset = 0
    for turn, artifact in validated:
        raw = _read_nonempty_text(artifact, "math_live_dialogue_artifact_invalid")
        evidence = (
            _assistant_section(raw, str(turn["timestamp"]))
            if turn["speaker"] == "assistant"
            else raw.strip()
        )
        source_text = messages[(str(turn["timestamp"]), str(turn["speaker"]))]
        if evidence not in source_text:
            _fail("math_live_dialogue_provenance_mismatch")
        if evidence == source_text:
            exact += 1
        else:
            subset += 1
    return {
        "session_id": value["session_id"],
        "source_rollout": str(rollout_path.resolve()),
        "source_binding": "exact_timestamp_role_turn_set",
        "source_turns_sha256": selected_turns_sha,
        "turn_count": len(validated),
        "exact_turn_count": exact,
        "verbatim_subset_turn_count": subset,
        "fabricated_turn_count": 0,
        "source_verified": True,
    }


def _validate_derived_freeze(
    contract: Mapping[str, Any],
    *,
    current_root: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    separation = contract.get("separation")
    if not isinstance(separation, Mapping):
        _fail("math_live_derived_fixture_binding_invalid")
    raw_previous_root = Path(str(separation.get("previous_test_fixture") or ""))
    if (
        not raw_previous_root.is_absolute()
        or raw_previous_root.is_symlink()
        or separation.get("write_to_previous_test_fixture") is not False
    ):
        _fail("math_live_derived_fixture_binding_invalid")
    try:
        previous_root = raw_previous_root.resolve(strict=True)
    except OSError as exc:
        raise MathLiveBusinessFixtureError(
            "math_live_derived_fixture_binding_invalid"
        ) from exc
    if previous_root == current_root or not previous_root.is_dir():
        _fail("math_live_derived_fixture_binding_invalid")
    manifest_path = previous_root / "read-only-test-manifest.json"
    manifest = _load_json(
        manifest_path,
        "math_live_derived_fixture_manifest_invalid",
    )
    authorization = manifest.get("authorization")
    samples = manifest.get("samples")
    frozen_at = manifest.get("frozen_at")
    old_separation = manifest.get("separation")
    if (
        manifest.get("schema_version")
        != "math-read-only-zero-model-test-manifest-v1"
        or Path(str(manifest.get("fixture_root") or "")).resolve(strict=True)
        != previous_root
        or manifest.get("use_mode") != "read_only_zero_model_preflight"
        or not isinstance(frozen_at, str)
        or ISO_TIMESTAMP_RE.fullmatch(frozen_at) is None
        or not isinstance(authorization, Mapping)
        or authorization.get("quick_intake_written") is not False
        or authorization.get("formal_write_count") != 0
        or authorization.get("processing_authorized") is not False
        or authorization.get("luna_authorized") is not False
        or authorization.get("sol_authorized") is not False
        or authorization.get("write_back_to_fixture") is not False
        or not isinstance(samples, list)
        or manifest.get("sample_count") != len(samples)
        or not isinstance(old_separation, Mapping)
        or Path(str(old_separation.get("new_capture_root") or "")).resolve(
            strict=True
        )
        != current_root
    ):
        _fail("math_live_derived_fixture_manifest_invalid")

    index: dict[str, dict[str, Any]] = {}
    for sample in samples:
        if not isinstance(sample, Mapping):
            _fail("math_live_derived_fixture_sample_invalid")
        raw_sample_path = Path(str(sample.get("absolute_path") or ""))
        identity = sample.get("question_identity")
        files = sample.get("files")
        if (
            not raw_sample_path.is_absolute()
            or raw_sample_path.is_symlink()
            or sample.get("independent_capture") is not True
            or sample.get("duplicate_of") is not None
            or not isinstance(identity, Mapping)
            or not isinstance(files, list)
        ):
            _fail("math_live_derived_fixture_sample_invalid")
        sample_path = _inside(
            previous_root,
            raw_sample_path,
            "math_live_derived_fixture_sample_invalid",
        )
        if not sample_path.is_dir() or sample_path.parent != previous_root / "items":
            _fail("math_live_derived_fixture_sample_invalid")
        question_rows = [
            row
            for row in files
            if isinstance(row, Mapping) and row.get("role") == "question_image"
        ]
        if len(question_rows) != 1:
            _fail("math_live_derived_fixture_question_invalid")
        question_row = question_rows[0]
        relative = _safe_relative(
            question_row.get("relative_path"),
            "math_live_derived_fixture_question_invalid",
        )
        declared_sha = _valid_sha(
            question_row.get("sha256"),
            "math_live_derived_fixture_question_invalid",
        )
        question_path = _inside(
            sample_path,
            sample_path / relative,
            "math_live_derived_fixture_question_invalid",
        )
        _verify_png(question_path, "math_live_derived_fixture_question_invalid")
        if _sha256(question_path) != declared_sha:
            _fail("math_live_derived_fixture_question_hash_mismatch")
        key = str(sample_path)
        if key in index:
            _fail("math_live_derived_fixture_sample_duplicate")
        index[key] = {
            "staging_id": _safe_id(
                sample.get("staging_id"),
                "math_live_derived_fixture_sample_invalid",
            ),
            "formal_id": _safe_id(
                identity.get("formal_id"),
                "math_live_derived_fixture_sample_invalid",
            ),
            "question_sha256": declared_sha,
            "frozen_at": frozen_at,
            "frozen_manifest_path": str(manifest_path),
            "frozen_manifest_sha256": _sha256(manifest_path),
        }
    return (
        {
            "frozen_manifest_path": str(manifest_path),
            "frozen_manifest_sha256": _sha256(manifest_path),
            "frozen_at": frozen_at,
            "sample_count": len(index),
            "write_back_to_fixture": False,
        },
        index,
    )


def _validate_source_boundary(
    path: Path,
    *,
    declared_sha256: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if _sha256(path) != declared_sha256:
        _fail("math_live_source_boundary_hash_mismatch")
    value = _load_json(path, "math_live_source_boundary_invalid")
    existing = value.get("existing_library_review_samples")
    new_sources = value.get("new_source_learning_samples")
    hard_boundaries = value.get("hard_boundaries")
    if (
        value.get("schema_version") != "math-sample-source-boundary-v1"
        or not isinstance(existing, list)
        or not isinstance(new_sources, list)
        or not isinstance(hard_boundaries, list)
        or not all(isinstance(row, str) and row for row in hard_boundaries)
    ):
        _fail("math_live_source_boundary_invalid")
    index: dict[str, dict[str, Any]] = {}
    for row in existing:
        if not isinstance(row, Mapping):
            _fail("math_live_source_boundary_invalid")
        capture_id = _safe_id(
            row.get("capture_id"), "math_live_source_boundary_invalid"
        )
        formal_id = _safe_id(
            row.get("formal_id"), "math_live_source_boundary_invalid"
        )
        if (
            capture_id in index
            or row.get("route") != "existing_formal_card_review"
            or not isinstance(row.get("obsidian_card_path"), str)
            or not Path(str(row["obsidian_card_path"])).is_absolute()
            or "Do not allocate a new formal ID" not in str(row.get("identity_rule"))
        ):
            _fail("math_live_source_boundary_invalid")
        index[capture_id] = {
            "business_task_id": row.get("business_task_id"),
            "source_route": "existing_formal_card_review",
            "formal_id": formal_id,
            "source_locator": f"formal_card:{formal_id}",
            "formal_id_must_remain_null": False,
            "obsidian_card_path": row["obsidian_card_path"],
        }
    for row in new_sources:
        if not isinstance(row, Mapping):
            _fail("math_live_source_boundary_invalid")
        capture_id = _safe_id(
            row.get("capture_id"), "math_live_source_boundary_invalid"
        )
        source_locator = _safe_id(
            row.get("source_locator"), "math_live_source_boundary_invalid"
        )
        if (
            capture_id in index
            or row.get("route") != "new_source_learning_episode"
            or row.get("formal_id") is not None
            or row.get("repository_search_result")
            != "no_confirmed_existing_formal_card_found"
            or "do not invent" not in str(row.get("identity_rule")).casefold()
        ):
            _fail("math_live_source_boundary_invalid")
        index[capture_id] = {
            "business_task_id": row.get("business_task_id"),
            "source_route": "new_source_learning_episode",
            "formal_id": None,
            "source_locator": source_locator,
            "formal_id_must_remain_null": True,
        }
    if set(index) != REAL_AUTHORITY_CAPTURE_IDS:
        _fail("math_live_source_boundary_scope_invalid")
    return (
        {
            "path": str(path),
            "sha256": declared_sha256,
            "existing_library_review_count": len(existing),
            "new_source_learning_count": len(new_sources),
            "capture_ids": sorted(index),
        },
        index,
    )


def _validate_superseded_entrypoints(
    paths: Sequence[Path],
    *,
    authoritative_manifest: Path,
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for path in paths:
        value = _load_json(path, "math_live_superseded_manifest_invalid")
        try:
            successor = Path(str(value.get("superseded_by") or "")).resolve(
                strict=True
            )
        except OSError as exc:
            raise MathLiveBusinessFixtureError(
                "math_live_superseded_manifest_invalid"
            ) from exc
        if (
            value.get("status") != "superseded_do_not_consume"
            or successor != authoritative_manifest
        ):
            _fail("math_live_superseded_manifest_invalid")
        results.append({"path": str(path), "sha256": _sha256(path)})
    return results


def _validate_contract(
    path: Path,
    *,
    current_root: Path,
    tasks: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    value = _load_json(path, "math_live_root_contract_invalid")
    required = {
        "schema_version",
        "state",
        "canonical_status",
        "policy",
        "authorized_business_task_exceptions",
        "storage",
        "current_item_count",
    }
    if not required.issubset(value):
        _fail("math_live_root_contract_invalid")
    policy = value.get("policy")
    exceptions = value.get("authorized_business_task_exceptions")
    if (
        value.get("schema_version") != "math-deferred-live-capture-hold-v1"
        or value.get("state") != "collecting"
        or value.get("canonical_status") != "noncanonical_deferred_holding_only"
        or not isinstance(policy, Mapping)
        or policy.get("quick_intake_written") is not False
        or policy.get("formal_write_count") != 0
        or policy.get("processing_authorized") is not False
        or policy.get("immutable_artifacts") is not True
        or policy.get("overwrite_existing_artifacts") is not False
        or not isinstance(exceptions, list)
        or value.get("current_item_count") != len(tasks)
    ):
        _fail("math_live_root_contract_invalid")
    expected = {
        (
            str(task["business_task_id"]),
            str(task["capture_id"]),
            True,
            True,
            False,
            0,
        )
        for task in tasks
    }
    actual: set[tuple[Any, ...]] = set()
    for row in exceptions:
        if not isinstance(row, Mapping):
            _fail("math_live_root_contract_exception_invalid")
        _expect_keys(
            row,
            {
                "business_task_id",
                "capture_id",
                "processing_authorized",
                "luna_authorized",
                "sol_authorized",
                "formal_write_count",
            },
            "math_live_root_contract_exception_invalid",
        )
        actual.add(
            (
                row.get("business_task_id"),
                row.get("capture_id"),
                row.get("processing_authorized"),
                row.get("luna_authorized"),
                row.get("sol_authorized"),
                row.get("formal_write_count"),
            )
        )
    if actual != expected or len(exceptions) != len(expected):
        _fail("math_live_root_contract_exception_invalid")
    derived_summary, derived_index = _validate_derived_freeze(
        value,
        current_root=current_root,
    )
    return {
        "schema_version": value["schema_version"],
        "default_processing_authorized": False,
        "authorized_exception_count": len(exceptions),
        "quick_intake_written": False,
        "formal_write_count": 0,
        "derived_freeze": derived_summary,
    }, derived_index


def _validate_record(
    path: Path,
    *,
    task: Mapping[str, Any],
    task_kind: str,
    item_files: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    value = _load_json(path, "math_live_record_invalid")
    required = {
        "schema_version",
        "capture_id",
        "business_task_id",
        "study_date",
        "state",
        "question_identity",
        "result",
        "solution_evidence",
        "business_task",
        "model_request",
        "authorization",
        "privacy",
        "artifacts",
        "missing_artifacts",
        "blocking_missing_artifacts",
    }
    if not required.issubset(value):
        _fail("math_live_record_keys_invalid")
    identity = value.get("question_identity")
    business = value.get("business_task")
    authorization = value.get("authorization")
    if (
        value.get("schema_version") != RECORD_SCHEMA
        or value.get("capture_id") != task["capture_id"]
        or value.get("business_task_id") != task["business_task_id"]
        or value.get("study_date") != "2026-08-09"
        or value.get("state") != "ready_for_luna_business_processing"
        or not isinstance(identity, Mapping)
        or identity.get("formal_id") != task["formal_id"]
        or identity.get("source_locator") != f"formal_card:{task['formal_id']}"
        or not isinstance(business, Mapping)
        or business.get("task_kind") != task_kind
        or not isinstance(authorization, Mapping)
        or authorization.get("processing_authorized") is not True
        or authorization.get("luna_authorized") is not True
        or authorization.get("sol_authorized") is not False
        or authorization.get("quick_intake_written") is not False
        or authorization.get("formal_write_count") != 0
        or _model_request(value.get("model_request"), "math_live_record_model_invalid")
        != MODEL_REQUEST
        or value.get("blocking_missing_artifacts") != []
    ):
        _fail("math_live_record_contract_invalid")
    luna_two_stage = business.get("luna_two_stage")
    if (
        not isinstance(luna_two_stage, Mapping)
        or luna_two_stage.get("stage_1") != "evidence_grounded_analysis"
        or luna_two_stage.get("stage_2") != "independent_critical_review"
        or luna_two_stage.get("adoption_mode") != "proposal_only"
    ):
        _fail("math_live_record_proposal_boundary_invalid")
    missing = value.get("missing_artifacts")
    if not isinstance(missing, list) or any(
        not isinstance(row, Mapping) or row.get("required") is not False for row in missing
    ):
        _fail("math_live_record_missing_artifact_invalid")

    declared = {
        (
            row.get("relative_path"),
            row.get("role"),
            row.get("visibility"),
            row.get("sha256"),
        )
        for row in item_files
        if isinstance(row, Mapping) and row.get("role") != "business_task_record"
    }
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list):
        _fail("math_live_record_artifacts_invalid")
    actual = set()
    for row in artifacts:
        if not isinstance(row, Mapping):
            _fail("math_live_record_artifacts_invalid")
        _expect_keys(
            row,
            {"relative_path", "role", "visibility", "sha256"},
            "math_live_record_artifacts_invalid",
        )
        actual.add(
            (
                row.get("relative_path"),
                row.get("role"),
                row.get("visibility"),
                row.get("sha256"),
            )
        )
    if actual != declared or len(artifacts) != len(declared):
        _fail("math_live_record_artifacts_invalid")

    result = value.get("result")
    if not isinstance(result, Mapping):
        _fail("math_live_record_result_invalid")
    if task_kind == "independent_correct_no_false_wrong_card":
        expected_outputs = business.get("expected_outputs")
        if (
            result.get("classification") != "correct"
            or result.get("independent_original_success") is not True
            or result.get("reasoning_transcript_status") != "not_provided"
            or result.get("reasoning_inference_allowed") is not False
            or not isinstance(expected_outputs, list)
            or "do_not_create_wrong_card_recommendation" not in expected_outputs
            or "explicit_unknowns_for_unobserved_reasoning" not in expected_outputs
            or "proposal_only_learning_observation" not in expected_outputs
            or "evidence_capsule" in value
        ):
            _fail("math_live_independent_correct_boundary_invalid")
        return {
            "classification": "correct",
            "independent_original_success": True,
            "reasoning_inference_allowed": False,
            "expected_no_false_wrong_card": True,
        }

    capsule = value.get("evidence_capsule")
    transfer = result.get("post_explanation_transfer")
    expected_outputs = business.get("expected_outputs")
    if (
        result.get("classification") != "wrong_then_corrected_after_explanation"
        or result.get("independent_original_success") is not False
        or not isinstance(transfer, Mapping)
        or transfer.get("result") != "correct"
        or not isinstance(capsule, Mapping)
        or not isinstance(capsule.get("independent_correct_steps"), list)
        or not capsule["independent_correct_steps"]
        or not isinstance(capsule.get("first_break"), str)
        or not capsule["first_break"].strip()
        or not isinstance(capsule.get("later_breaks"), list)
        or not capsule["later_breaks"]
        or not isinstance(capsule.get("resolution"), str)
        or not capsule["resolution"].strip()
        or not isinstance(expected_outputs, list)
        or "first_break_and_later_breaks" not in expected_outputs
        or "independent_correct_steps" not in expected_outputs
        or "post_explanation_understanding_boundary" not in expected_outputs
        or "weakness_distillation_proposal" not in expected_outputs
        or "no_formal_write" not in expected_outputs
    ):
        _fail("math_live_weakness_proposal_boundary_invalid")
    return {
        "classification": "wrong_then_corrected_after_explanation",
        "independent_original_success": False,
        "first_break_present": True,
        "later_break_count": len(capsule["later_breaks"]),
        "independent_correct_step_count": len(capsule["independent_correct_steps"]),
        "post_explanation_transfer_result": "correct",
    }


def _validate_derived_chain(
    record: Mapping[str, Any],
    *,
    task: Mapping[str, Any],
    item_files: Sequence[Mapping[str, Any]],
    derived_index: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    raw_derived = Path(str(record.get("derived_from_frozen_capture") or ""))
    if not raw_derived.is_absolute() or raw_derived.is_symlink():
        _fail("math_live_derived_capture_path_invalid")
    try:
        derived_path = raw_derived.resolve(strict=True)
    except OSError as exc:
        raise MathLiveBusinessFixtureError(
            "math_live_derived_capture_path_invalid"
        ) from exc
    binding = derived_index.get(str(derived_path))
    question_rows = [
        row
        for row in item_files
        if isinstance(row, Mapping) and row.get("role") == "question_image"
    ]
    if (
        not isinstance(binding, Mapping)
        or len(question_rows) != 1
        or binding.get("formal_id") != task.get("formal_id")
        or binding.get("question_sha256") != question_rows[0].get("sha256")
        or record.get("study_date") != "2026-08-09"
    ):
        _fail("math_live_derived_capture_binding_mismatch")
    frozen_at = binding.get("frozen_at")
    if not isinstance(frozen_at, str) or ISO_TIMESTAMP_RE.fullmatch(frozen_at) is None:
        _fail("math_live_derived_capture_timestamp_invalid")
    return {
        "derived_staging_id": binding["staging_id"],
        "derived_capture_path": str(derived_path),
        "frozen_manifest_path": binding["frozen_manifest_path"],
        "frozen_manifest_sha256": binding["frozen_manifest_sha256"],
        "captured_at": frozen_at,
        "formal_id_match": True,
        "question_sha256_match": True,
    }


def _validate_solution(
    *,
    item_dir: Path,
    item_manifest: Mapping[str, Any],
    item_files: Sequence[Mapping[str, Any]],
    record: Mapping[str, Any],
    trusted_source_root: Path,
) -> dict[str, Any]:
    text_rows = [row for row in item_files if row.get("role") == "solution_text"]
    image_rows = [row for row in item_files if row.get("role") == "solution_image"]
    policy = item_manifest.get("solution_policy")
    evidence = record.get("solution_evidence")
    identity = record.get("question_identity")
    if (
        len(text_rows) > 1
        or len(image_rows) > 1
        or not text_rows + image_rows
        or not isinstance(policy, Mapping)
        or policy.get("requirement") != SOLUTION_REQUIREMENT
        or not isinstance(evidence, Mapping)
        or evidence.get("accepted_policy") != SOLUTION_REQUIREMENT
        or not isinstance(identity, Mapping)
    ):
        _fail("math_live_solution_evidence_invalid")

    satisfied_by = policy.get("satisfied_by")
    declared_paths = {row.get("relative_path") for row in text_rows + image_rows}
    if satisfied_by not in declared_paths:
        _fail("math_live_solution_evidence_invalid")
    if evidence.get("solution_text_present") is not bool(text_rows) or evidence.get(
        "solution_image_present"
    ) is not bool(image_rows):
        _fail("math_live_solution_evidence_invalid")

    source_path: str | None = None
    source_sha: str | None = None
    if text_rows:
        row = text_rows[0]
        relative = str(row["relative_path"])
        path = _inside(item_dir, item_dir / relative, "math_live_solution_text_invalid")
        frontmatter, _ = _frontmatter(path)
        expected_frontmatter = {
            "role",
            "visibility",
            "source_kind",
            "source_path",
            "source_sha256",
            "solution_image_present",
            "solution_image_absence",
        }
        _expect_keys(
            frontmatter,
            expected_frontmatter,
            "math_live_solution_frontmatter_keys_invalid",
        )
        if (
            frontmatter.get("role") != "solution_text"
            or frontmatter.get("visibility") != "private_evidence"
            or frontmatter.get("source_kind") != "obsidian_visual_detail_card"
            or frontmatter.get("solution_image_present") != "false"
            or frontmatter.get("solution_image_absence")
            != "authentic_source_has_text_solution_only"
        ):
            _fail("math_live_solution_frontmatter_invalid")
        raw_source = Path(str(frontmatter.get("source_path") or ""))
        if not raw_source.is_absolute() or raw_source.is_symlink():
            _fail("math_live_solution_source_path_invalid")
        source = _inside(
            trusted_source_root,
            raw_source,
            "math_live_solution_source_path_invalid",
        )
        if source.is_symlink() or not source.is_file() or source.stat().st_size <= 0:
            _fail("math_live_solution_source_path_invalid")
        source_sha = _valid_sha(
            frontmatter.get("source_sha256"),
            "math_live_solution_source_hash_invalid",
        )
        if (
            _sha256(source) != source_sha
            or identity.get("formal_card_path") != str(source)
            or identity.get("formal_card_sha256") != source_sha
            or evidence.get("source_verified") is not True
        ):
            _fail("math_live_solution_source_hash_mismatch")
        source_path = str(source)
    else:
        if evidence.get("source_verified") is not True:
            _fail("math_live_solution_evidence_invalid")

    for row in image_rows:
        path = _inside(
            item_dir,
            item_dir / str(row["relative_path"]),
            "math_live_solution_image_invalid",
        )
        _verify_png(path, "math_live_solution_image_invalid")
    if not image_rows and (
        policy.get("solution_image_present") is not False
        or policy.get("absence_is_authentic_business_case") is not True
        or evidence.get("solution_image_absence")
        != "authentic_obsidian_source_contains_text_solution_but_no_solution_image"
    ):
        _fail("math_live_solution_image_absence_invalid")
    return {
        "accepted_policy": SOLUTION_REQUIREMENT,
        "satisfied_by": satisfied_by,
        "solution_text_present": bool(text_rows),
        "solution_image_present": bool(image_rows),
        "source_path": source_path,
        "source_sha256": source_sha,
        "source_verified": True,
    }


def _validate_item(
    task: Mapping[str, Any],
    *,
    root: Path,
    trusted_source_root: Path,
    trusted_rollout_root: Path,
    derived_index: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    expected_task_keys = {
        "business_task_id",
        "capture_id",
        "formal_id",
        "absolute_path",
        "manifest_path",
        "manifest_sha256",
        "content_fingerprint",
        "task_kind",
        "blocking_missing_artifacts",
    }
    _expect_keys(task, expected_task_keys, "math_live_batch_task_keys_invalid")
    capture_id = _safe_id(task.get("capture_id"), "math_live_capture_id_invalid")
    business_task_id = _safe_id(
        task.get("business_task_id"), "math_live_business_task_id_invalid"
    )
    formal_id = _safe_id(task.get("formal_id"), "math_live_formal_id_invalid")
    task_kind = task.get("task_kind")
    if task_kind not in TASK_RULES or task.get("blocking_missing_artifacts") != []:
        _fail("math_live_task_contract_invalid")
    item_dir = _inside(
        root,
        Path(str(task.get("absolute_path") or "")),
        "math_live_task_path_invalid",
    )
    if item_dir.is_symlink() or not item_dir.is_dir() or item_dir.parent != root / "items":
        _fail("math_live_task_path_invalid")
    if item_dir.name != capture_id:
        _fail("math_live_task_path_invalid")
    manifest_path = _inside(
        item_dir,
        Path(str(task.get("manifest_path") or "")),
        "math_live_item_manifest_path_invalid",
    )
    if manifest_path != item_dir / "manifest.json":
        _fail("math_live_item_manifest_path_invalid")
    manifest_sha = _valid_sha(
        task.get("manifest_sha256"), "math_live_item_manifest_hash_invalid"
    )
    if _sha256(manifest_path) != manifest_sha:
        _fail("math_live_item_manifest_hash_mismatch")

    manifest = _load_json(manifest_path, "math_live_item_manifest_invalid")
    _expect_keys(
        manifest,
        {
            "schema_version",
            "capture_id",
            "business_task_id",
            "formal_id",
            "state",
            "independent_capture",
            "duplicate_of",
            "source_fixture_preserved",
            "content_fingerprint",
            "authorization",
            "solution_policy",
            "files",
            "missing_artifacts_consistent_with_record",
            "blocking_missing_artifacts",
        },
        "math_live_item_manifest_keys_invalid",
    )
    fingerprint_node = manifest.get("content_fingerprint")
    if (
        manifest.get("schema_version") != ITEM_SCHEMA
        or manifest.get("capture_id") != capture_id
        or manifest.get("business_task_id") != business_task_id
        or manifest.get("formal_id") != formal_id
        or manifest.get("state") != "sealed_ready_for_luna"
        or manifest.get("independent_capture") is not True
        or manifest.get("duplicate_of") is not None
        or manifest.get("source_fixture_preserved") is not True
        or not isinstance(fingerprint_node, Mapping)
        or fingerprint_node.get("algorithm") != FINGERPRINT_ALGORITHM
        or manifest.get("missing_artifacts_consistent_with_record") is not True
        or manifest.get("blocking_missing_artifacts") != []
    ):
        _fail("math_live_item_manifest_contract_invalid")
    _authorization(
        manifest.get("authorization"),
        include_quick=False,
        code="math_live_item_authorization_invalid",
    )
    declared_fingerprint = _valid_sha(
        fingerprint_node.get("value"), "math_live_item_fingerprint_invalid"
    )
    if task.get("content_fingerprint") != declared_fingerprint:
        _fail("math_live_item_fingerprint_mismatch")

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        _fail("math_live_item_files_invalid")
    declared_paths: set[str] = set()
    rows: list[tuple[str, str]] = []
    normalized_roles: set[str] = set()
    artifact_descriptors: list[dict[str, str]] = []
    record_path: Path | None = None
    dialogue_path: Path | None = None
    for row in files:
        if not isinstance(row, Mapping):
            _fail("math_live_item_file_invalid")
        _expect_keys(
            row,
            {"relative_path", "role", "visibility", "sha256"},
            "math_live_item_file_keys_invalid",
        )
        relative = _safe_relative(row.get("relative_path"), "math_live_item_file_invalid")
        role = row.get("role")
        visibility = row.get("visibility")
        declared_sha = _valid_sha(row.get("sha256"), "math_live_item_file_invalid")
        if (
            relative == "manifest.json"
            or relative in declared_paths
            or role not in ROLE_VISIBILITY
            or ROLE_VISIBILITY[role] != visibility
        ):
            _fail("math_live_item_file_invalid")
        path = _inside(item_dir, item_dir / relative, "math_live_item_file_path_invalid")
        try:
            node = path.lstat()
        except OSError as exc:
            raise MathLiveBusinessFixtureError("math_live_item_file_missing") from exc
        if path.is_symlink() or not path.is_file() or node.st_size <= 0:
            _fail("math_live_item_file_invalid")
        if _sha256(path) != declared_sha:
            _fail("math_live_item_file_hash_mismatch")
        if role in {"question_image", "solution_image"}:
            _verify_png(path, "math_live_item_image_invalid")
        elif role == "business_task_record":
            if path.name != "record.json" or record_path is not None:
                _fail("math_live_record_path_invalid")
            record_path = path
        elif role == "dialogue_sequence_and_provenance":
            if path.name != "dialogue_index.json" or dialogue_path is not None:
                _fail("math_live_dialogue_file_invalid")
            dialogue_path = path
        else:
            _read_nonempty_text(path, "math_live_text_artifact_invalid")
        declared_paths.add(relative)
        rows.append((relative, declared_sha))
        artifact_descriptors.append(
            {
                "artifact_id": f"{capture_id}:{relative}",
                "kind": str(role),
                "path": str(path),
                "sha256": declared_sha,
                "visibility": str(visibility),
            }
        )
        normalized_roles.add("solution_evidence" if role in {"solution_text", "solution_image"} else str(role))
    if normalized_roles != TASK_RULES[str(task_kind)]["roles"]:
        _fail("math_live_task_role_set_invalid")
    actual_paths = {
        str(path.relative_to(item_dir))
        for path in item_dir.rglob("*")
        if path.is_file()
    }
    expected_paths = declared_paths | {"manifest.json"}
    if actual_paths != expected_paths:
        _fail("math_live_item_file_set_mismatch")
    if record_path is None or dialogue_path is None:
        _fail("math_live_task_required_file_missing")
    computed_fingerprint = _fingerprint(rows)
    if computed_fingerprint != declared_fingerprint:
        _fail("math_live_item_fingerprint_mismatch")

    record_value = _load_json(record_path, "math_live_record_invalid")
    record_summary = _validate_record(
        record_path,
        task=task,
        task_kind=str(task_kind),
        item_files=files,
    )
    derived_binding = _validate_derived_chain(
        record_value,
        task=task,
        item_files=files,
        derived_index=derived_index,
    )
    solution = _validate_solution(
        item_dir=item_dir,
        item_manifest=manifest,
        item_files=files,
        record=record_value,
        trusted_source_root=trusted_source_root,
    )
    dialogue = _validate_dialogue(
        dialogue_path,
        item_dir=item_dir,
        declared_paths=declared_paths,
        task_kind=str(task_kind),
        trusted_rollout_root=trusted_rollout_root,
    )
    return {
        "capture_id": capture_id,
        "business_task_id": business_task_id,
        "formal_id": formal_id,
        "task_kind": task_kind,
        "content_fingerprint": computed_fingerprint,
        "manifest_sha256": manifest_sha,
        "artifacts": sorted(
            artifact_descriptors, key=lambda row: str(row["artifact_id"])
        ),
        "solution_evidence": solution,
        "dialogue_provenance": dialogue,
        "result_boundary": record_summary,
        "study_date": record_value["study_date"],
        "captured_at": derived_binding["captured_at"],
        "derived_capture_binding": derived_binding,
        "reasoning_boundary": TASK_RULES[str(task_kind)]["reasoning_boundary"],
        "luna_eligible": True,
        "proposal_only": True,
        "model_request": dict(MODEL_REQUEST),
        "authorization": {
            "processing_authorized": True,
            "luna_authorized": True,
            "sol_authorized": False,
            "formal_write_count": 0,
            "quick_intake_written": False,
        },
    }


def _validate_real_contract(
    path: Path,
    *,
    current_root: Path,
    tasks: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], str]:
    value = _load_json(path, "math_live_real_contract_invalid")
    _expect_keys(
        value,
        {
            "schema_version",
            "study_date",
            "state",
            "canonical_status",
            "created_at",
            "separation",
            "policy",
            "authorized_business_task_exceptions",
            "storage",
            "current_item_count",
            "notes",
        },
        "math_live_real_contract_keys_invalid",
    )
    created_at = value.get("created_at")
    separation = value.get("separation")
    policy = value.get("policy")
    exceptions = value.get("authorized_business_task_exceptions")
    storage = value.get("storage")
    if (
        value.get("schema_version") != "math-luna-real-business-sample-root-v1"
        or value.get("study_date") != "2026-08-09"
        or value.get("state") != "active_real_luna_business_testing"
        or value.get("canonical_status") != "noncanonical_operational_test_inputs"
        or not isinstance(created_at, str)
        or ISO_TIMESTAMP_RE.fullmatch(created_at) is None
        or not isinstance(separation, Mapping)
        or separation.get("write_to_previous_test_fixture") is not False
        or separation.get("sample_directory_mode")
        != "operational_writable_for_test_state_and_outputs"
        or separation.get("new_study_question_writes_to_this_root") is not False
        or separation.get("test_runtime_writes_to_this_root") is not True
        or not isinstance(separation.get("next_capture_root"), str)
        or not Path(str(separation["next_capture_root"])).is_absolute()
        or not isinstance(policy, Mapping)
        or policy.get("luna_processing_authorized") is not True
        or policy.get("luna_local_database_read_authorized") is not True
        or policy.get("luna_output_role") != "preprocessing_proposal"
        or policy.get("luna_formal_library_write_authorized") is not False
        or policy.get("sol_formal_library_writer") is not True
        or policy.get("sol_execution_authorization")
        != "governed_by_downstream_formal_workflow"
        or policy.get("quick_intake_authorization")
        != "governed_by_live_intake_system"
        or policy.get("current_formal_write_count") != 0
        or not isinstance(policy.get("new_study_capture_scope"), Mapping)
        or policy["new_study_capture_scope"].get("enabled") is not False
        or "do not invent a formal ID"
        not in str(policy["new_study_capture_scope"].get("identity_rule"))
        or value.get("current_item_count") != 3
        or not isinstance(exceptions, list)
        or len(exceptions) != 3
        or not isinstance(storage, Mapping)
        or storage.get("items_directory") != "items"
        or storage.get("shared_mutable_manifest") is not False
    ):
        _fail("math_live_real_contract_invalid")
    expected = {
        (task.get("business_task_id"), task.get("capture_id")) for task in tasks
    }
    actual: set[tuple[Any, Any]] = set()
    for row in exceptions:
        if not isinstance(row, Mapping):
            _fail("math_live_real_contract_exception_invalid")
        _expect_keys(
            row,
            {
                "business_task_id",
                "capture_id",
                "luna_processing_authorized",
                "luna_local_database_read_authorized",
                "luna_formal_library_write_authorized",
                "sol_formal_library_writer",
                "quick_intake_authorization",
            },
            "math_live_real_contract_exception_invalid",
        )
        if (
            row.get("luna_processing_authorized") is not True
            or row.get("luna_local_database_read_authorized") is not True
            or row.get("luna_formal_library_write_authorized") is not False
            or row.get("sol_formal_library_writer") is not True
            or row.get("quick_intake_authorization")
            != "governed_by_live_intake_system"
        ):
            _fail("math_live_real_contract_exception_invalid")
        actual.add((row.get("business_task_id"), row.get("capture_id")))
    if actual != expected or len(actual) != len(exceptions):
        _fail("math_live_real_contract_exception_invalid")
    derived_summary, derived_index = _validate_derived_freeze(
        value,
        current_root=current_root,
    )
    return (
        {
            "schema_version": value["schema_version"],
            "study_date": value["study_date"],
            "state": value["state"],
            "canonical_status": value["canonical_status"],
            "sample_directory_mode": separation["sample_directory_mode"],
            "new_study_question_writes_to_this_root": False,
            "test_runtime_writes_to_this_root": True,
            "luna_processing_authorized": True,
            "luna_local_database_read_authorized": True,
            "luna_output_role": "preprocessing_proposal",
            "luna_formal_library_write_authorized": False,
            "sol_execution_authorization": "governed_by_downstream_formal_workflow",
            "formal_write_count": 0,
            "authorized_exception_count": len(actual),
            "derived_freeze": derived_summary,
        },
        derived_index,
        created_at,
    )


def _validate_real_dialogue(
    path: Path,
    *,
    item_dir: Path,
    declared_paths: set[str],
    task_kind: str,
    trusted_rollout_root: Path,
) -> dict[str, Any]:
    value = _load_json(path, "math_live_dialogue_index_invalid")
    if "source_rollout" in value:
        return _validate_dialogue(
            path,
            item_dir=item_dir,
            declared_paths=declared_paths,
            task_kind=task_kind,
            trusted_rollout_root=trusted_rollout_root,
        )
    _expect_keys(
        value,
        {
            "schema_version",
            "session_id",
            "turns",
            "reasoning_transcript_status",
            "fabricated_turn_count",
        },
        "math_live_dialogue_index_keys_invalid",
    )
    rule = TASK_RULES[task_kind]
    turns = value.get("turns")
    if (
        task_kind != "wrong_then_corrected_multistage_method_proposal"
        or value.get("schema_version") != DIALOGUE_SCHEMA
        or _safe_id(value.get("session_id"), "math_live_dialogue_session_invalid")
        is None
        or value.get("reasoning_transcript_status") != rule["reasoning_status"]
        or value.get("fabricated_turn_count") != 0
        or not isinstance(turns, list)
        or len(turns) != len(rule["dialogue_files"])
    ):
        _fail("math_live_dialogue_contract_invalid")
    turn_bindings: list[dict[str, Any]] = []
    for sequence, turn in enumerate(turns, start=1):
        if not isinstance(turn, Mapping):
            _fail("math_live_dialogue_turn_invalid")
        speaker = turn.get("speaker")
        expected_keys = {"sequence", "speaker", "file"}
        expected_keys.add("evidence_kind" if speaker == "user" else "section")
        _expect_keys(turn, expected_keys, "math_live_dialogue_turn_keys_invalid")
        relative = _safe_relative(
            turn.get("file"), "math_live_dialogue_file_invalid"
        )
        if (
            turn.get("sequence") != sequence
            or speaker != rule["dialogue_speakers"][sequence - 1]
            or relative != rule["dialogue_files"][sequence - 1]
            or relative not in declared_paths
        ):
            _fail("math_live_dialogue_turn_invalid")
        artifact = _inside(
            item_dir,
            item_dir / relative,
            "math_live_dialogue_file_invalid",
        )
        raw = _read_nonempty_text(
            artifact, "math_live_dialogue_artifact_invalid"
        )
        if speaker == "user":
            if not isinstance(turn.get("evidence_kind"), str) or not str(
                turn["evidence_kind"]
            ).strip():
                _fail("math_live_dialogue_turn_invalid")
        else:
            section = turn.get("section")
            if not isinstance(section, str) or not section.strip():
                _fail("math_live_dialogue_turn_invalid")
            _assistant_section(raw, section)
        turn_bindings.append(
            {
                "sequence": sequence,
                "speaker": speaker,
                "file_sha256": _sha256(artifact),
            }
        )
    selected_sha = hashlib.sha256(
        json.dumps(
            turn_bindings,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "session_id": value["session_id"],
        "source_rollout": None,
        "source_binding": "authority_manifest_hash_and_local_dialogue_index",
        "source_turns_sha256": selected_sha,
        "turn_count": len(turn_bindings),
        "exact_turn_count": len(turn_bindings),
        "verbatim_subset_turn_count": 0,
        "fabricated_turn_count": 0,
        "source_verified": True,
        "external_rollout_locator_present": False,
    }


def _validate_real_record(
    path: Path,
    *,
    task: Mapping[str, Any],
    task_kind: str,
    source_binding: Mapping[str, Any],
    item_files: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    value = _load_json(path, "math_live_record_invalid")
    required = {
        "schema_version",
        "capture_id",
        "business_task_id",
        "study_date",
        "state",
        "question_identity",
        "result",
        "solution_evidence",
        "business_task",
        "model_request",
        "runtime_authority",
        "privacy",
        "artifacts",
        "missing_artifacts",
        "blocking_missing_artifacts",
    }
    allowed = set(required)
    if task_kind != "independent_correct_no_false_wrong_card":
        allowed.add("evidence_capsule")
    if source_binding["source_route"] == "existing_formal_card_review":
        allowed.add("derived_from_frozen_capture")
    _expect_keys(value, allowed, "math_live_record_keys_invalid")
    identity = value.get("question_identity")
    business = value.get("business_task")
    privacy = value.get("privacy")
    if (
        value.get("schema_version") != RECORD_SCHEMA
        or value.get("capture_id") != task.get("capture_id")
        or value.get("business_task_id") != task.get("business_task_id")
        or value.get("study_date") != "2026-08-09"
        or value.get("state") != "ready_for_luna_business_processing"
        or not isinstance(identity, Mapping)
        or identity.get("formal_id") != source_binding["formal_id"]
        or identity.get("source_locator") != source_binding["source_locator"]
        or not isinstance(business, Mapping)
        or business.get("task_kind") != task_kind
        or not isinstance(privacy, Mapping)
        or privacy.get("visibility") != "private_evidence"
        or privacy.get("question_image_answer_safe") is not True
        or not any(
            privacy.get(key) is False
            for key in (
                "solution_and_feedback_learner_facing",
                "solution_and_dialogue_learner_facing",
            )
        )
        or _model_request(value.get("model_request"), "math_live_record_model_invalid")
        != MODEL_REQUEST
        or value.get("blocking_missing_artifacts") != []
    ):
        _fail("math_live_record_contract_invalid")
    _runtime_authority(
        value.get("runtime_authority"), "math_live_record_authority_invalid"
    )
    if source_binding["formal_id_must_remain_null"]:
        if (
            identity.get("kind") != "new_source"
            or identity.get("formal_id") is not None
            or identity.get("invented_formal_id") is not False
            or "formal_card_path" in identity
            or "formal_card_sha256" in identity
        ):
            _fail("math_live_new_source_formal_id_invented")
    else:
        if (
            identity.get("formal_card_path")
            != source_binding.get("obsidian_card_path")
            or not isinstance(identity.get("formal_card_sha256"), str)
        ):
            _fail("math_live_existing_source_identity_invalid")
    luna_two_stage = business.get("luna_two_stage")
    expected_outputs = business.get("expected_outputs")
    if (
        not isinstance(luna_two_stage, Mapping)
        or luna_two_stage.get("stage_1") != "evidence_grounded_analysis"
        or luna_two_stage.get("stage_2") != "independent_critical_review"
        or luna_two_stage.get("adoption_mode") != "proposal_only"
        or not isinstance(expected_outputs, list)
    ):
        _fail("math_live_record_proposal_boundary_invalid")
    missing = value.get("missing_artifacts")
    if not isinstance(missing, list) or any(
        not isinstance(row, Mapping) or row.get("required") is not False
        for row in missing
    ):
        _fail("math_live_record_missing_artifact_invalid")
    declared = {
        (
            row.get("relative_path"),
            row.get("role"),
            row.get("visibility"),
            row.get("sha256"),
        )
        for row in item_files
        if isinstance(row, Mapping) and row.get("role") != "business_task_record"
    }
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list):
        _fail("math_live_record_artifacts_invalid")
    actual: set[tuple[Any, ...]] = set()
    for row in artifacts:
        if not isinstance(row, Mapping):
            _fail("math_live_record_artifacts_invalid")
        _expect_keys(
            row,
            {"relative_path", "role", "visibility", "sha256"},
            "math_live_record_artifacts_invalid",
        )
        actual.add(
            (
                row.get("relative_path"),
                row.get("role"),
                row.get("visibility"),
                row.get("sha256"),
            )
        )
    if actual != declared or len(artifacts) != len(declared):
        _fail("math_live_record_artifacts_invalid")
    result = value.get("result")
    if not isinstance(result, Mapping):
        _fail("math_live_record_result_invalid")
    if task_kind == "independent_correct_no_false_wrong_card":
        if (
            result.get("classification") != "correct"
            or result.get("independent_original_success") is not True
            or result.get("reasoning_transcript_status") != "not_provided"
            or result.get("reasoning_inference_allowed") is not False
            or "do_not_create_wrong_card_recommendation" not in expected_outputs
            or "explicit_unknowns_for_unobserved_reasoning" not in expected_outputs
            or "proposal_only_learning_observation" not in expected_outputs
        ):
            _fail("math_live_independent_correct_boundary_invalid")
        summary = {
            "classification": "correct",
            "independent_original_success": True,
            "reasoning_inference_allowed": False,
            "expected_no_false_wrong_card": True,
        }
    else:
        capsule = value.get("evidence_capsule")
        if (
            result.get("classification") != "wrong_then_corrected_after_explanation"
            or result.get("independent_original_success") is not False
            or not isinstance(capsule, Mapping)
            or not isinstance(capsule.get("independent_correct_steps"), list)
            or not capsule["independent_correct_steps"]
            or not isinstance(capsule.get("first_break"), str)
            or not capsule["first_break"].strip()
            or not isinstance(capsule.get("later_breaks"), list)
            or not capsule["later_breaks"]
            or not isinstance(capsule.get("resolution"), str)
            or not capsule["resolution"].strip()
            or "first_break_and_later_breaks" not in expected_outputs
            or "independent_correct_steps" not in expected_outputs
            or "post_explanation_understanding_boundary" not in expected_outputs
            or "no_formal_write" not in expected_outputs
        ):
            _fail("math_live_weakness_proposal_boundary_invalid")
        if task_kind == "wrong_then_corrected_weakness_proposal":
            transfer = result.get("post_explanation_transfer")
            if (
                not isinstance(transfer, Mapping)
                or transfer.get("result") != "correct"
                or "weakness_distillation_proposal" not in expected_outputs
            ):
                _fail("math_live_weakness_proposal_boundary_invalid")
        elif (
            "geometry_object_role_confusion" not in expected_outputs
            or "riemann_sum_mapping_confusion" not in expected_outputs
        ):
            _fail("math_live_multistage_boundary_invalid")
        summary = {
            "classification": "wrong_then_corrected_after_explanation",
            "independent_original_success": False,
            "first_break_present": True,
            "later_break_count": len(capsule["later_breaks"]),
            "independent_correct_step_count": len(capsule["independent_correct_steps"]),
            "post_explanation_understanding_present": True,
        }
    return value, summary


def _validate_real_solution(
    *,
    item_dir: Path,
    item_manifest: Mapping[str, Any],
    item_files: Sequence[Mapping[str, Any]],
    record: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    trusted_source_root: Path,
) -> dict[str, Any]:
    text_rows = [row for row in item_files if row.get("role") == "solution_text"]
    image_rows = [row for row in item_files if row.get("role") == "solution_image"]
    policy = item_manifest.get("solution_policy")
    evidence = record.get("solution_evidence")
    if (
        len(text_rows) > 1
        or len(image_rows) > 1
        or not text_rows + image_rows
        or not isinstance(policy, Mapping)
        or policy.get("requirement") != SOLUTION_REQUIREMENT
        or not isinstance(evidence, Mapping)
        or evidence.get("accepted_policy") != SOLUTION_REQUIREMENT
        or evidence.get("solution_text_present") is not bool(text_rows)
        or evidence.get("solution_image_present") is not bool(image_rows)
    ):
        _fail("math_live_solution_evidence_invalid")
    declared_paths = {str(row.get("relative_path")) for row in text_rows + image_rows}
    satisfied_by = policy.get("satisfied_by")
    satisfied_set = {satisfied_by} if isinstance(satisfied_by, str) else set(
        satisfied_by if isinstance(satisfied_by, list) else []
    )
    if not satisfied_set or satisfied_set != declared_paths:
        _fail("math_live_solution_evidence_invalid")
    for row in image_rows:
        image_path = _inside(
            item_dir,
            item_dir / str(row["relative_path"]),
            "math_live_solution_image_invalid",
        )
        _verify_png(image_path, "math_live_solution_image_invalid")
    source_path: str | None = None
    source_sha: str | None = None
    source_kind: str
    source_locator = source_binding["source_locator"]
    if source_binding["source_route"] == "existing_formal_card_review":
        if len(text_rows) != 1 or image_rows:
            _fail("math_live_solution_evidence_invalid")
        frontmatter, _ = _frontmatter(
            item_dir / str(text_rows[0]["relative_path"])
        )
        _expect_keys(
            frontmatter,
            {
                "role",
                "visibility",
                "source_kind",
                "source_path",
                "source_sha256",
                "solution_image_present",
                "solution_image_absence",
            },
            "math_live_solution_frontmatter_keys_invalid",
        )
        if (
            frontmatter.get("role") != "solution_text"
            or frontmatter.get("visibility") != "private_evidence"
            or frontmatter.get("source_kind") != "obsidian_visual_detail_card"
            or frontmatter.get("solution_image_present") != "false"
            or frontmatter.get("solution_image_absence")
            != "authentic_source_has_text_solution_only"
            or policy.get("solution_image_present") is not False
            or policy.get("absence_is_authentic_business_case") is not True
            or evidence.get("source_verified") is not True
        ):
            _fail("math_live_solution_frontmatter_invalid")
        raw_source = Path(str(frontmatter.get("source_path") or ""))
        if not raw_source.is_absolute() or raw_source.is_symlink():
            _fail("math_live_solution_source_path_invalid")
        source = _inside(
            trusted_source_root,
            raw_source,
            "math_live_solution_source_path_invalid",
        )
        source_sha = _valid_sha(
            frontmatter.get("source_sha256"),
            "math_live_solution_source_hash_invalid",
        )
        identity = record.get("question_identity")
        if (
            _sha256(source) != source_sha
            or not isinstance(identity, Mapping)
            or identity.get("formal_card_path") != str(source)
            or identity.get("formal_card_sha256") != source_sha
            or source_binding.get("obsidian_card_path") != str(source)
        ):
            _fail("math_live_solution_source_hash_mismatch")
        source_path = str(source)
        source_kind = "obsidian_visual_detail_card"
    else:
        if len(text_rows) != 1 or len(image_rows) != 1:
            _fail("math_live_solution_evidence_invalid")
        frontmatter, _ = _frontmatter(
            item_dir / str(text_rows[0]["relative_path"])
        )
        _expect_keys(
            frontmatter,
            {
                "role",
                "visibility",
                "source_kind",
                "source_locator",
                "solution_image_present",
            },
            "math_live_solution_frontmatter_keys_invalid",
        )
        if (
            frontmatter.get("role") != "solution_text"
            or frontmatter.get("visibility") != "private_evidence"
            or frontmatter.get("source_kind") != "user_supplied_solution_image"
            or frontmatter.get("source_locator") != source_locator
            or frontmatter.get("solution_image_present") != "true"
            or policy.get("solution_image_present") is not True
            or evidence.get("source_verified_from_user_attachment") is not True
        ):
            _fail("math_live_solution_frontmatter_invalid")
        source_sha = str(image_rows[0]["sha256"])
        source_kind = "user_supplied_solution_image"
    return {
        "accepted_policy": SOLUTION_REQUIREMENT,
        "satisfied_by": sorted(satisfied_set),
        "solution_text_present": bool(text_rows),
        "solution_image_present": bool(image_rows),
        "source_kind": source_kind,
        "source_locator": source_locator,
        "source_path": source_path,
        "source_sha256": source_sha,
        "source_verified": True,
    }


def _validate_real_item(
    task: Mapping[str, Any],
    *,
    root: Path,
    source_index: Mapping[str, Mapping[str, Any]],
    trusted_source_root: Path,
    trusted_rollout_root: Path,
    derived_index: Mapping[str, Mapping[str, Any]],
    authority_created_at: str,
) -> dict[str, Any]:
    base_task_keys = {
        "business_task_id",
        "capture_id",
        "source_route",
        "formal_id",
        "absolute_path",
        "manifest_path",
        "manifest_sha256",
        "content_fingerprint",
        "task_kind",
        "blocking_missing_artifacts",
    }
    expected_task_keys = set(base_task_keys)
    if task.get("source_route") == "new_source_learning_episode":
        expected_task_keys.add("source_locator")
    _expect_keys(task, expected_task_keys, "math_live_authority_task_keys_invalid")
    capture_id = _safe_id(task.get("capture_id"), "math_live_capture_id_invalid")
    business_task_id = _safe_id(
        task.get("business_task_id"), "math_live_business_task_id_invalid"
    )
    binding = source_index.get(capture_id)
    task_kind = task.get("task_kind")
    if (
        not isinstance(binding, Mapping)
        or binding.get("business_task_id") != business_task_id
        or task.get("source_route") != binding.get("source_route")
        or task.get("formal_id") != binding.get("formal_id")
        or task.get("source_locator", binding.get("source_locator"))
        != binding.get("source_locator")
        or task_kind not in TASK_RULES
        or task.get("blocking_missing_artifacts") != []
    ):
        _fail("math_live_authority_task_contract_invalid")
    formal_id: str | None
    if binding["formal_id_must_remain_null"]:
        if task.get("formal_id") is not None:
            _fail("math_live_new_source_formal_id_invented")
        formal_id = None
    else:
        formal_id = _safe_id(task.get("formal_id"), "math_live_formal_id_invalid")
    item_dir = _inside(
        root,
        Path(str(task.get("absolute_path") or "")),
        "math_live_task_path_invalid",
    )
    if item_dir.is_symlink() or not item_dir.is_dir() or item_dir.parent != root / "items":
        _fail("math_live_task_path_invalid")
    if item_dir.name != capture_id:
        _fail("math_live_task_path_invalid")
    manifest_path = _inside(
        item_dir,
        Path(str(task.get("manifest_path") or "")),
        "math_live_item_manifest_path_invalid",
    )
    if manifest_path != item_dir / "manifest.json":
        _fail("math_live_item_manifest_path_invalid")
    manifest_sha = _valid_sha(
        task.get("manifest_sha256"), "math_live_item_manifest_hash_invalid"
    )
    if _sha256(manifest_path) != manifest_sha:
        _fail("math_live_item_manifest_hash_mismatch")
    manifest = _load_json(manifest_path, "math_live_item_manifest_invalid")
    item_keys = {
        "schema_version",
        "capture_id",
        "business_task_id",
        "formal_id",
        "state",
        "directory_mode",
        "independent_capture",
        "duplicate_of",
        "content_fingerprint",
        "runtime_authority",
        "solution_policy",
        "files",
        "missing_artifacts_consistent_with_record",
        "blocking_missing_artifacts",
    }
    if binding["formal_id_must_remain_null"]:
        item_keys.add("source_locator")
    else:
        item_keys.add("source_fixture_preserved")
    _expect_keys(manifest, item_keys, "math_live_item_manifest_keys_invalid")
    fingerprint_node = manifest.get("content_fingerprint")
    if (
        manifest.get("schema_version") != ITEM_SCHEMA
        or manifest.get("capture_id") != capture_id
        or manifest.get("business_task_id") != business_task_id
        or manifest.get("formal_id") != formal_id
        or manifest.get("source_locator", binding.get("source_locator"))
        != binding.get("source_locator")
        or manifest.get("state") != "ready_for_real_luna_business_processing"
        or manifest.get("directory_mode")
        != "operational_writable_for_test_state_and_outputs"
        or manifest.get("independent_capture") is not True
        or manifest.get("duplicate_of") is not None
        or (
            not binding["formal_id_must_remain_null"]
            and manifest.get("source_fixture_preserved") is not True
        )
        or not isinstance(fingerprint_node, Mapping)
        or fingerprint_node.get("algorithm") != FINGERPRINT_ALGORITHM
        or manifest.get("missing_artifacts_consistent_with_record") is not True
        or manifest.get("blocking_missing_artifacts") != []
    ):
        _fail("math_live_item_manifest_contract_invalid")
    _runtime_authority(
        manifest.get("runtime_authority"), "math_live_item_authority_invalid"
    )
    declared_fingerprint = _valid_sha(
        fingerprint_node.get("value"), "math_live_item_fingerprint_invalid"
    )
    if task.get("content_fingerprint") != declared_fingerprint:
        _fail("math_live_item_fingerprint_mismatch")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        _fail("math_live_item_files_invalid")
    declared_paths: set[str] = set()
    rows: list[tuple[str, str]] = []
    normalized_roles: set[str] = set()
    descriptors: list[dict[str, str]] = []
    record_path: Path | None = None
    dialogue_path: Path | None = None
    for row in files:
        if not isinstance(row, Mapping):
            _fail("math_live_item_file_invalid")
        _expect_keys(
            row,
            {"relative_path", "role", "visibility", "sha256"},
            "math_live_item_file_keys_invalid",
        )
        relative = _safe_relative(row.get("relative_path"), "math_live_item_file_invalid")
        role = row.get("role")
        visibility = row.get("visibility")
        declared_sha = _valid_sha(row.get("sha256"), "math_live_item_file_invalid")
        if (
            relative == "manifest.json"
            or relative in declared_paths
            or role not in ROLE_VISIBILITY
            or ROLE_VISIBILITY[role] != visibility
        ):
            _fail("math_live_item_file_invalid")
        artifact = _inside(item_dir, item_dir / relative, "math_live_item_file_path_invalid")
        try:
            node = artifact.lstat()
        except OSError as exc:
            raise MathLiveBusinessFixtureError("math_live_item_file_missing") from exc
        if artifact.is_symlink() or not artifact.is_file() or node.st_size <= 0:
            _fail("math_live_item_file_invalid")
        if _sha256(artifact) != declared_sha:
            _fail("math_live_item_file_hash_mismatch")
        if role in {"question_image", "solution_image"}:
            _verify_png(artifact, "math_live_item_image_invalid")
        elif role == "business_task_record":
            if artifact.name != "record.json" or record_path is not None:
                _fail("math_live_record_path_invalid")
            record_path = artifact
        elif role == "dialogue_sequence_and_provenance":
            if artifact.name != "dialogue_index.json" or dialogue_path is not None:
                _fail("math_live_dialogue_file_invalid")
            dialogue_path = artifact
        else:
            _read_nonempty_text(artifact, "math_live_text_artifact_invalid")
        declared_paths.add(relative)
        rows.append((relative, declared_sha))
        descriptors.append(
            {
                "artifact_id": f"{capture_id}:{relative}",
                "kind": str(role),
                "path": str(artifact),
                "sha256": declared_sha,
                "visibility": str(visibility),
            }
        )
        normalized_roles.add(
            "solution_evidence" if role in {"solution_text", "solution_image"} else str(role)
        )
    if normalized_roles != TASK_RULES[str(task_kind)]["roles"]:
        _fail("math_live_task_role_set_invalid")
    actual_paths: set[str] = set()
    for path in item_dir.rglob("*"):
        if path.is_symlink():
            _fail("math_live_fixture_symlink_forbidden")
        if path.is_file():
            actual_paths.add(str(path.relative_to(item_dir)))
        elif not path.is_dir():
            _fail("math_live_fixture_special_file_forbidden")
    if actual_paths != declared_paths | {"manifest.json"}:
        _fail("math_live_item_file_set_mismatch")
    if record_path is None or dialogue_path is None:
        _fail("math_live_task_required_file_missing")
    computed_fingerprint = _fingerprint(rows)
    if computed_fingerprint != declared_fingerprint:
        _fail("math_live_item_fingerprint_mismatch")
    record, record_summary = _validate_real_record(
        record_path,
        task=task,
        task_kind=str(task_kind),
        source_binding=binding,
        item_files=files,
    )
    if binding["formal_id_must_remain_null"]:
        if "derived_from_frozen_capture" in record:
            _fail("math_live_new_source_derived_from_formal_fixture")
        derived_binding = {
            "source_route": binding["source_route"],
            "source_locator": binding["source_locator"],
            "formal_id": None,
            "formal_id_must_remain_null": True,
            "captured_at": authority_created_at,
        }
    else:
        derived_binding = _validate_derived_chain(
            record,
            task=task,
            item_files=files,
            derived_index=derived_index,
        )
    solution = _validate_real_solution(
        item_dir=item_dir,
        item_manifest=manifest,
        item_files=files,
        record=record,
        source_binding=binding,
        trusted_source_root=trusted_source_root,
    )
    dialogue = _validate_real_dialogue(
        dialogue_path,
        item_dir=item_dir,
        declared_paths=declared_paths,
        task_kind=str(task_kind),
        trusted_rollout_root=trusted_rollout_root,
    )
    return {
        "capture_id": capture_id,
        "business_task_id": business_task_id,
        "formal_id": formal_id,
        "source_route": binding["source_route"],
        "source_locator": binding["source_locator"],
        "formal_id_must_remain_null": bool(binding["formal_id_must_remain_null"]),
        "task_kind": task_kind,
        "content_fingerprint": computed_fingerprint,
        "manifest_sha256": manifest_sha,
        "artifacts": sorted(descriptors, key=lambda row: row["artifact_id"]),
        "solution_evidence": solution,
        "dialogue_provenance": dialogue,
        "result_boundary": record_summary,
        "study_date": record["study_date"],
        "captured_at": derived_binding["captured_at"],
        "derived_capture_binding": derived_binding,
        "reasoning_boundary": TASK_RULES[str(task_kind)]["reasoning_boundary"],
        "luna_eligible": True,
        "proposal_only": True,
        "model_request": dict(MODEL_REQUEST),
        "authorization": {
            "processing_authorized": True,
            "luna_authorized": True,
            "sol_authorized": False,
            "formal_write_count": 0,
            "quick_intake_written": False,
        },
    }


def _validate_real_manifest(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    *,
    trusted_source_root: Path,
    trusted_rollout_root: Path,
) -> dict[str, Any]:
    _expect_keys(
        manifest,
        {
            "schema_version",
            "authoritative_entrypoint",
            "created_at",
            "root",
            "state",
            "directory_mode",
            "task_count",
            "supersedes",
            "source_boundary",
            "runtime_contract",
            "runtime_authority",
            "model_request",
            "tasks",
            "storage_routing",
        },
        "math_live_authority_manifest_keys_invalid",
    )
    try:
        root = Path(str(manifest.get("root") or "")).expanduser().resolve(strict=True)
    except OSError as exc:
        raise MathLiveBusinessFixtureError("math_live_authority_root_invalid") from exc
    tasks = manifest.get("tasks")
    source_boundary_node = manifest.get("source_boundary")
    contract_node = manifest.get("runtime_contract")
    storage = manifest.get("storage_routing")
    created_at = manifest.get("created_at")
    if (
        manifest_path != root / "luna-real-business-samples.json"
        or manifest.get("schema_version") != REAL_AUTHORITY_SCHEMA
        or manifest.get("authoritative_entrypoint") is not True
        or not isinstance(created_at, str)
        or ISO_TIMESTAMP_RE.fullmatch(created_at) is None
        or manifest.get("state") != "ready_for_real_luna_business_testing"
        or manifest.get("directory_mode")
        != "operational_writable_for_test_state_and_outputs"
        or not isinstance(tasks, list)
        or len(tasks) != 3
        or manifest.get("task_count") != 3
        or not isinstance(source_boundary_node, Mapping)
        or not isinstance(contract_node, Mapping)
        or not isinstance(storage, Mapping)
        or storage.get("sample_root_frozen") is not False
        or storage.get("new_study_questions_use_separate_root") is not True
        or not isinstance(storage.get("future_capture_root"), str)
        or not Path(str(storage["future_capture_root"])).is_absolute()
    ):
        _fail("math_live_authority_contract_invalid")
    _runtime_authority(
        manifest.get("runtime_authority"), "math_live_authority_invalid"
    )
    model_request = _model_request(
        manifest.get("model_request"), "math_live_authority_model_invalid"
    )
    capture_ids = {
        str(row.get("capture_id")) for row in tasks if isinstance(row, Mapping)
    }
    if capture_ids != REAL_AUTHORITY_CAPTURE_IDS or len(capture_ids) != len(tasks):
        _fail("math_live_authority_scope_invalid")
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, list) or len(supersedes) != 2:
        _fail("math_live_authority_supersedes_invalid")
    try:
        superseded_paths = [Path(str(value)).resolve(strict=True) for value in supersedes]
        source_boundary_path = Path(str(source_boundary_node.get("path") or "")).resolve(
            strict=True
        )
        contract_path = Path(str(contract_node.get("path") or "")).resolve(strict=True)
    except OSError as exc:
        raise MathLiveBusinessFixtureError("math_live_authority_path_invalid") from exc
    expected_superseded = {
        root / "luna-business-task-manifest.json",
        root / "luna-business-task-addendum-001.json",
    }
    if (
        set(superseded_paths) != expected_superseded
        or source_boundary_path != root / "source-boundary-note.json"
        or contract_path != root / "contract.json"
    ):
        _fail("math_live_authority_path_invalid")
    source_boundary_sha = _valid_sha(
        source_boundary_node.get("sha256"), "math_live_source_boundary_hash_invalid"
    )
    contract_sha = _valid_sha(
        contract_node.get("sha256"), "math_live_real_contract_hash_invalid"
    )
    authority_files = [
        manifest_path,
        source_boundary_path,
        contract_path,
        *superseded_paths,
    ]
    expected_root_entries = {
        "contract.json",
        "items",
        "luna-business-task-addendum-001.json",
        "luna-business-task-manifest.json",
        "luna-real-business-samples.json",
        "source-boundary-note.json",
    }
    root_entries = list(root.iterdir())
    if any(path.is_symlink() for path in root_entries):
        _fail("math_live_fixture_symlink_forbidden")
    if {path.name for path in root_entries} != expected_root_entries:
        _fail("math_live_fixture_root_file_set_mismatch")
    actual_item_ids, out_of_scope_ids = _item_directory_scope(
        root / "items", listed_capture_ids=capture_ids
    )
    if actual_item_ids != capture_ids or out_of_scope_ids:
        _fail("math_live_authority_item_directory_set_mismatch")
    before = _authority_snapshot(
        root,
        authority_files=authority_files,
        capture_ids=capture_ids,
    )
    if _sha256(contract_path) != contract_sha:
        _fail("math_live_real_contract_hash_mismatch")
    contract, derived_index, contract_created_at = _validate_real_contract(
        contract_path,
        current_root=root,
        tasks=tasks,
    )
    if contract_created_at > created_at:
        _fail("math_live_authority_timestamp_invalid")
    source_boundary, source_index = _validate_source_boundary(
        source_boundary_path,
        declared_sha256=source_boundary_sha,
    )
    superseded = _validate_superseded_entrypoints(
        superseded_paths,
        authoritative_manifest=manifest_path,
    )
    seen_capture: set[str] = set()
    seen_business: set[str] = set()
    seen_formal: set[str] = set()
    seen_fingerprint: set[str] = set()
    results: list[dict[str, Any]] = []
    authority_sha = _sha256(manifest_path)
    batch_id = f"MATH-LUNA-REAL-20260809-{authority_sha[:12]}"
    for task in tasks:
        if not isinstance(task, Mapping):
            _fail("math_live_authority_task_invalid")
        result = _validate_real_item(
            task,
            root=root,
            source_index=source_index,
            trusted_source_root=trusted_source_root,
            trusted_rollout_root=trusted_rollout_root,
            derived_index=derived_index,
            authority_created_at=created_at,
        )
        result["batch_id"] = batch_id
        for value, seen in (
            (result["capture_id"], seen_capture),
            (result["business_task_id"], seen_business),
            (result["content_fingerprint"], seen_fingerprint),
        ):
            if value in seen:
                _fail("math_live_task_identity_not_distinct")
            seen.add(str(value))
        if result["formal_id"] is not None:
            if result["formal_id"] in seen_formal:
                _fail("math_live_task_identity_not_distinct")
            seen_formal.add(str(result["formal_id"]))
        results.append(result)
    after = _authority_snapshot(
        root,
        authority_files=authority_files,
        capture_ids=capture_ids,
    )
    if before != after:
        _fail("math_live_fixture_changed_during_validation")
    return {
        "schema_version": RESULT_SCHEMA,
        "status": "passed_real_luna_business_preflight",
        "authority_schema_version": REAL_AUTHORITY_SCHEMA,
        "batch_id": batch_id,
        "batch_manifest_path": str(manifest_path),
        "batch_manifest_sha256": authority_sha,
        "authority_manifest_path": str(manifest_path),
        "authority_manifest_sha256": authority_sha,
        "contract_path": str(contract_path),
        "contract_sha256": contract_sha,
        "contract": contract,
        "source_boundary": source_boundary,
        "superseded_entrypoints": sorted(superseded, key=lambda row: row["path"]),
        "task_count": len(results),
        "distinct_capture_count": len(seen_capture),
        "existing_formal_id_count": len(seen_formal),
        "new_source_without_formal_id_count": sum(
            1 for row in results if row["formal_id"] is None
        ),
        "business_sample_type": "real_math_learning_captures",
        "solution_evidence_policy": {
            "requirement": SOLUTION_REQUIREMENT,
            "validated_task_count": len(results),
            "solution_text_task_count": sum(
                1 for row in results if row["solution_evidence"]["solution_text_present"]
            ),
            "solution_image_task_count": sum(
                1 for row in results if row["solution_evidence"]["solution_image_present"]
            ),
        },
        "model_request": model_request,
        "authorization": {
            "processing_authorized": True,
            "luna_authorized": True,
            "sol_authorized": False,
            "formal_write_count": 0,
            "quick_intake_written_by_validator": False,
        },
        "tasks": results,
        "fixture_tree_sha256": _snapshot_sha(before),
        "fixture_tree_scope": "authority_contract_boundary_superseded_bindings_and_all_three_task_directories",
        "fixture_tree_before_after": "unchanged",
        "out_of_scope_item_directory_count": 0,
        "out_of_scope_item_directory_ids": [],
        "out_of_scope_item_contents_read_count": 0,
        "fixture_symlink_count": 0,
        "fixture_write_count": 0,
        "mcp_tool_call_count": 0,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def validate_manifest(
    manifest_path: Path,
    *,
    trusted_source_root: Path | None = None,
    trusted_rollout_root: Path | None = None,
) -> dict[str, Any]:
    """Validate a batch and return private-text-free, inspectable metadata."""

    try:
        manifest_path = manifest_path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise MathLiveBusinessFixtureError("math_live_batch_manifest_missing") from exc
    manifest = _load_json(manifest_path, "math_live_batch_manifest_invalid")
    if manifest.get("status") == "superseded_do_not_consume":
        _fail("math_live_manifest_superseded")
    if manifest.get("schema_version") == REAL_AUTHORITY_SCHEMA:
        try:
            source_root = (
                trusted_source_root or DEFAULT_TRUSTED_SOURCE_ROOT
            ).expanduser().resolve(strict=True)
            rollout_root = (
                trusted_rollout_root or DEFAULT_TRUSTED_ROLLOUT_ROOT
            ).expanduser().resolve(strict=True)
        except OSError as exc:
            raise MathLiveBusinessFixtureError(
                "math_live_trusted_root_invalid"
            ) from exc
        return _validate_real_manifest(
            manifest_path,
            manifest,
            trusted_source_root=source_root,
            trusted_rollout_root=rollout_root,
        )
    _expect_keys(
        manifest,
        {
            "schema_version",
            "batch_id",
            "root",
            "state",
            "task_count",
            "business_sample_type",
            "solution_evidence_policy",
            "authorization",
            "model_request",
            "tasks",
            "safety",
        },
        "math_live_batch_manifest_keys_invalid",
    )
    root = Path(str(manifest.get("root") or "")).expanduser().resolve(strict=True)
    if manifest_path != root / "luna-business-task-manifest.json":
        _fail("math_live_batch_manifest_path_invalid")
    tasks = manifest.get("tasks")
    policy = manifest.get("solution_evidence_policy")
    safety = manifest.get("safety")
    if (
        manifest.get("schema_version") != BATCH_SCHEMA
        or _safe_id(manifest.get("batch_id"), "math_live_batch_id_invalid") is None
        or manifest.get("state") != "ready_for_luna_business_processing"
        or manifest.get("business_sample_type") != "real_math_learning_captures"
        or not isinstance(tasks, list)
        or not tasks
        or manifest.get("task_count") != len(tasks)
        or not isinstance(policy, Mapping)
        or policy.get("requirement") != SOLUTION_REQUIREMENT
        or policy.get("solution_images_fabricated") is not False
        or policy.get("solution_text_copied_with_source_path_and_hash") is not True
        or not isinstance(safety, Mapping)
        or safety.get("frozen_source_fixture_unchanged") is not True
        or safety.get("private_solution_and_dialogue_not_learner_facing") is not True
        or safety.get("luna_output_proposal_only") is not True
        or safety.get("formal_mutation_authorized") is not False
    ):
        _fail("math_live_batch_contract_invalid")
    authorization = _authorization(
        manifest.get("authorization"),
        include_quick=True,
        code="math_live_batch_authorization_invalid",
    )
    model_request = _model_request(
        manifest.get("model_request"), "math_live_batch_model_invalid"
    )
    source_root = (trusted_source_root or DEFAULT_TRUSTED_SOURCE_ROOT).expanduser().resolve(
        strict=True
    )
    rollout_root = (
        trusted_rollout_root or DEFAULT_TRUSTED_ROLLOUT_ROOT
    ).expanduser().resolve(strict=True)

    expected_root_entries = {"contract.json", "items", manifest_path.name}
    root_entries = list(root.iterdir())
    if any(path.is_symlink() for path in root_entries):
        _fail("math_live_fixture_symlink_forbidden")
    if {path.name for path in root_entries} != expected_root_entries:
        _fail("math_live_fixture_root_file_set_mismatch")
    task_dirs = {str(task.get("capture_id")) for task in tasks if isinstance(task, Mapping)}
    _, out_of_scope_item_ids = _item_directory_scope(
        root / "items", listed_capture_ids=task_dirs
    )
    before = _scoped_snapshot(
        root,
        manifest_path=manifest_path,
        capture_ids=task_dirs,
    )

    contract_path = root / "contract.json"
    contract, derived_index = _validate_contract(
        contract_path,
        current_root=root,
        tasks=tasks,
    )
    seen_capture: set[str] = set()
    seen_business: set[str] = set()
    seen_formal: set[str] = set()
    seen_fingerprint: set[str] = set()
    results: list[dict[str, Any]] = []
    for task in tasks:
        if not isinstance(task, Mapping):
            _fail("math_live_batch_task_invalid")
        result = _validate_item(
            task,
            root=root,
            trusted_source_root=source_root,
            trusted_rollout_root=rollout_root,
            derived_index=derived_index,
        )
        result["batch_id"] = manifest["batch_id"]
        uniqueness = (
            ("capture", result["capture_id"], seen_capture),
            ("business", result["business_task_id"], seen_business),
            ("formal", result["formal_id"], seen_formal),
            ("fingerprint", result["content_fingerprint"], seen_fingerprint),
        )
        for _, value, seen in uniqueness:
            if value in seen:
                _fail("math_live_task_identity_not_distinct")
            seen.add(str(value))
        results.append(result)

    after = _scoped_snapshot(
        root,
        manifest_path=manifest_path,
        capture_ids=task_dirs,
    )
    if before != after:
        _fail("math_live_fixture_changed_during_validation")
    tree_sha = _snapshot_sha(before)
    return {
        "schema_version": RESULT_SCHEMA,
        "status": "passed_luna_business_preflight",
        "batch_id": manifest["batch_id"],
        "batch_manifest_path": str(manifest_path),
        "batch_manifest_sha256": _sha256(manifest_path),
        "contract_path": str(contract_path),
        "contract_sha256": _sha256(contract_path),
        "contract": contract,
        "task_count": len(results),
        "distinct_capture_count": len(seen_capture),
        "business_sample_type": manifest["business_sample_type"],
        "solution_evidence_policy": {
            "requirement": SOLUTION_REQUIREMENT,
            "validated_task_count": len(results),
            "solution_text_task_count": sum(
                1 for row in results if row["solution_evidence"]["solution_text_present"]
            ),
            "solution_image_task_count": sum(
                1 for row in results if row["solution_evidence"]["solution_image_present"]
            ),
        },
        "model_request": model_request,
        "authorization": authorization,
        "tasks": results,
        "fixture_tree_sha256": tree_sha,
        "fixture_tree_scope": "contract_batch_manifest_and_listed_task_directories_only",
        "fixture_tree_before_after": "unchanged",
        "out_of_scope_item_directory_count": len(out_of_scope_item_ids),
        "out_of_scope_item_directory_ids": out_of_scope_item_ids,
        "out_of_scope_item_contents_read_count": 0,
        "fixture_symlink_count": 0,
        "fixture_write_count": 0,
        "mcp_tool_call_count": 0,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def validated_task_to_processing_host_capture_args(
    task: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert one already validated task result into Host capture arguments.

    This function performs no filesystem access.  Local paths are present only
    in ``capture_artifacts`` so the Host can freeze the declared bytes.  The
    model-visible ``capture_facts`` contains no evidence body or local path.
    """

    if (
        task.get("luna_eligible") is not True
        or task.get("proposal_only") is not True
        or not isinstance(task.get("artifacts"), Sequence)
        or isinstance(task.get("artifacts"), (str, bytes, bytearray))
        or not isinstance(task.get("authorization"), Mapping)
        or task["authorization"].get("processing_authorized") is not True
        or task["authorization"].get("luna_authorized") is not True
        or task["authorization"].get("sol_authorized") is not False
        or task["authorization"].get("formal_write_count") != 0
        or task["authorization"].get("quick_intake_written") is not False
    ):
        _fail("math_live_capture_args_task_invalid")
    capture_id = _safe_id(
        task.get("capture_id"), "math_live_capture_args_identity_invalid"
    )
    business_task_id = _safe_id(
        task.get("business_task_id"), "math_live_capture_args_identity_invalid"
    )
    raw_formal_id = task.get("formal_id")
    raw_source_locator = task.get("source_locator")
    if not isinstance(raw_source_locator, str) and isinstance(raw_formal_id, str):
        raw_source_locator = f"formal_card:{raw_formal_id}"
    source_locator = _safe_id(
        raw_source_locator, "math_live_capture_args_identity_invalid"
    )
    if raw_formal_id is None:
        if (
            task.get("source_route") != "new_source_learning_episode"
            or task.get("formal_id_must_remain_null") is not True
        ):
            _fail("math_live_capture_args_identity_invalid")
        formal_id: str | None = None
    else:
        formal_id = _safe_id(
            raw_formal_id, "math_live_capture_args_identity_invalid"
        )
        if (
            task.get("source_route") not in {None, "existing_formal_card_review"}
            or source_locator != f"formal_card:{formal_id}"
        ):
            _fail("math_live_capture_args_identity_invalid")
    batch_id = _safe_id(
        task.get("batch_id"), "math_live_capture_args_identity_invalid"
    )
    task_kind = task.get("task_kind")
    study_date = task.get("study_date")
    captured_at = task.get("captured_at")
    content_fingerprint = _valid_sha(
        task.get("content_fingerprint"),
        "math_live_capture_args_identity_invalid",
    )
    manifest_sha = _valid_sha(
        task.get("manifest_sha256"),
        "math_live_capture_args_identity_invalid",
    )
    if (
        task_kind not in TASK_RULES
        or not isinstance(study_date, str)
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}", study_date) is None
        or not isinstance(captured_at, str)
        or ISO_TIMESTAMP_RE.fullmatch(captured_at) is None
    ):
        _fail("math_live_capture_args_identity_invalid")
    derived_binding = task.get("derived_capture_binding")
    if not isinstance(derived_binding, Mapping):
        _fail("math_live_capture_args_derived_binding_invalid")
    if formal_id is None:
        if (
            derived_binding.get("formal_id") is not None
            or derived_binding.get("formal_id_must_remain_null") is not True
            or derived_binding.get("source_locator") != source_locator
            or derived_binding.get("captured_at") != captured_at
        ):
            _fail("math_live_capture_args_derived_binding_invalid")
    elif (
        derived_binding.get("formal_id_match") is not True
        or derived_binding.get("question_sha256_match") is not True
        or derived_binding.get("captured_at") != captured_at
    ):
        _fail("math_live_capture_args_derived_binding_invalid")

    kind_map = {
        "question_image": "question_image",
        "solution_text": "solution_text",
        "solution_image": "solution_image",
        "business_task_record": "learning_record",
    }
    capture_artifacts: list[dict[str, str]] = []
    artifact_index: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for descriptor in task["artifacts"]:
        if not isinstance(descriptor, Mapping):
            _fail("math_live_capture_args_artifact_invalid")
        _expect_keys(
            descriptor,
            {"artifact_id", "kind", "path", "sha256", "visibility"},
            "math_live_capture_args_artifact_invalid",
        )
        artifact_id = _safe_id(
            descriptor.get("artifact_id"),
            "math_live_capture_args_artifact_invalid",
        )
        original_role = descriptor.get("kind")
        visibility = descriptor.get("visibility")
        sha256 = _valid_sha(
            descriptor.get("sha256"),
            "math_live_capture_args_artifact_invalid",
        )
        path = descriptor.get("path")
        if (
            artifact_id in seen_ids
            or not isinstance(original_role, str)
            or original_role not in ROLE_VISIBILITY
            or visibility != ROLE_VISIBILITY[original_role]
            or not isinstance(path, str)
            or not Path(path).is_absolute()
        ):
            _fail("math_live_capture_args_artifact_invalid")
        seen_ids.add(artifact_id)
        artifact_kind = kind_map.get(original_role, "dialogue")
        if artifact_kind == "dialogue" and visibility != "private_evidence":
            _fail("math_live_capture_args_artifact_invalid")
        capture_artifacts.append(
            {
                "artifact_id": artifact_id,
                "artifact_kind": artifact_kind,
                "path": path,
                "sha256": sha256,
            }
        )
        artifact_index.append(
            {
                "artifact_id": artifact_id,
                "artifact_kind": artifact_kind,
                "source_role": original_role,
                "sha256": sha256,
                "visibility": str(visibility),
            }
        )
    if len(capture_artifacts) != len(task["artifacts"]) or not capture_artifacts:
        _fail("math_live_capture_args_artifact_coverage_invalid")
    mapped_kinds = {row["artifact_kind"] for row in capture_artifacts}
    if (
        "question_image" not in mapped_kinds
        or not mapped_kinds.intersection({"solution_text", "solution_image"})
        or "learning_record" not in mapped_kinds
        or "dialogue" not in mapped_kinds
    ):
        _fail("math_live_capture_args_artifact_coverage_invalid")

    solution = task.get("solution_evidence")
    if not isinstance(solution, Mapping) or solution.get("source_verified") is not True:
        _fail("math_live_capture_args_solution_binding_invalid")
    solution_source_sha = solution.get("source_sha256")
    if solution_source_sha is not None:
        solution_source_sha = _valid_sha(
            solution_source_sha,
            "math_live_capture_args_solution_binding_invalid",
        )
    capture_facts = {
        "schema_version": "math-live-business-host-facts-v1",
        "facts": {
            "task": {
                "batch_id": batch_id,
                "business_task_id": business_task_id,
                "capture_id": capture_id,
                "task_kind": task_kind,
                "study_date": study_date,
                "reasoning_boundary": task.get("reasoning_boundary"),
            },
            "identity": {
                "formal_id": formal_id,
                "source_locator": source_locator,
                "content_fingerprint": content_fingerprint,
                "item_manifest_sha256": manifest_sha,
                "solution_source_sha256": solution_source_sha,
                "solution_source_verified": True,
            },
            "artifact_index": sorted(
                artifact_index, key=lambda row: str(row["artifact_id"])
            ),
        },
    }
    facts_raw = (
        json.dumps(
            capture_facts,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    decoded_facts = facts_raw.decode("utf-8")
    if any(
        marker in decoded_facts
        for marker in ("/Users/", "/Volumes/", "/private/", "/var/", "/tmp/")
    ):
        _fail("math_live_capture_args_facts_path_exposed")
    capture_identity = {
        "content_fingerprint": content_fingerprint,
    }
    capture_scene: str
    if formal_id is None:
        capture_identity["source_id"] = source_locator
        capture_scene = "new_intake"
    else:
        capture_identity["formal_id"] = formal_id
        capture_scene = "study_review"
    return {
        "capture_facts_sha256": hashlib.sha256(facts_raw).hexdigest(),
        "capture_facts": capture_facts,
        "capture_scene": capture_scene,
        "capture_identity": capture_identity,
        "capture_artifacts": tuple(
            sorted(capture_artifacts, key=lambda row: str(row["artifact_id"]))
        ),
        "captured_at": captured_at,
    }


def write_content_addressed_receipt(
    result: Mapping[str, Any],
    output_root: Path,
) -> tuple[Path, str]:
    """Write a deterministic receipt outside the fixture, idempotently."""

    raw = (
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    target = output_root.expanduser() / "sha256" / digest[:2] / f"{digest}.json"
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(target.parent, 0o700)
    if target.exists():
        if target.is_symlink() or not target.is_file() or target.read_bytes() != raw:
            _fail("math_live_receipt_collision")
        os.chmod(target, 0o400)
        return target.resolve(), digest
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o400)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()
    return target.resolve(), digest
