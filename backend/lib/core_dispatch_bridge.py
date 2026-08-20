#!/usr/bin/env python3
"""Bridge existing read-only adapters and per-candidate Workers to dispatch.

The bridge intentionally lives outside ``preprocessor_core``.  Candidate
discovery uses one scanner Worker; model execution and private publication use
a fresh Worker/CodexRunner pair for every claimed task and never acquire the
legacy daemon's global worker lock.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import inspect
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from concurrent_dispatch import (
    DispatchCancelled,
    DispatchError,
    FrozenTask,
    LeaseStore,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
    TaskExecutionContext,
    dispatch_rule_binding,
    validate_dispatch_rule_binding,
)
from execution_quality_contract import decide_execution_quality
from process_identity import (
    ProcessIdentityError,
    kernel_activity_snapshot_sha256,
    kernel_process_activity_delta,
    kernel_process_activity_snapshot,
    kernel_process_start_token,
)
from math_exact_smoke import (
    MathExactSmokeError,
    exact_task_binding_for_capture,
    pending_exact_smoke_dispatch_scope,
)
from skill_binding_contract import (
    SkillBindingError,
    bind_candidate_producer_attestations,
)
from preprocessor_core import (
    Candidate,
    CURRENT_QUESTION_BUNDLE_SCHEMA,
    CURRENT_QUESTION_BUNDLE_SCHEMA_V3,
    INTERACTION_TRACE_SCHEMA_V2,
    LEGACY_CURRENT_QUESTION_COMPATIBILITY_SCHEMA,
    LOADED_CORE_SHA256,
    ModelResult,
    PreprocessorError,
    Worker,
    cs408_processing_contract,
    current_question_bundle_evidence_status,
    current_date,
    english_processing_contract,
    math_group_processing_key,
    math_processing_contract,
    parse_time,
    sha256_value,
    subject_dispatch_bridge_code_closure_manifest_from_path,
    subject_semantic_code_closure_manifest_from_path,
    validate_current_question_bundle,
)


@dataclass(frozen=True)
class EligibleFrozenCandidate:
    task: FrozenTask
    candidate: Candidate
    reason: str


_RELEASE_SUBJECT_CONTRACT_CACHE: dict[
    tuple[str, str, str], dict[str, Any]
] = {}

PRODUCER_AUTHORITY_SCHEMA = "study-intake-producer-authority-v1"
PRODUCER_DISPATCH_INPUT_SCHEMA = "study-intake-producer-dispatch-input-v2"


def _subject_processing_contract(
    config: Mapping[str, Any], subject: str
) -> dict[str, Any]:
    builders = {
        "math": math_processing_contract,
        "cs408": cs408_processing_contract,
        "english": english_processing_contract,
    }
    builder = builders.get(subject)
    if builder is None:
        raise DispatchError("subject_processing_contract_invalid")
    contract = builder(config)
    if not isinstance(contract, Mapping):
        raise DispatchError("subject_processing_contract_missing")
    return copy.deepcopy(dict(contract))


def producer_authority_binding(
    config: Mapping[str, Any], subject: str, release_id: str
) -> dict[str, Any]:
    """Return a model-free binding for one subject's producer contract.

    This identifies the parser/adapter contract that authenticated the source
    capture.  It deliberately does not open an MCP session or inspect Luna
    state, so producer admission remains independent from the consumer.
    """

    if subject not in {"math", "cs408", "english"}:
        raise DispatchError("producer_authority_subject_invalid")
    if (
        not isinstance(release_id, str)
        or len(release_id) != 64
        or any(char not in "0123456789abcdef" for char in release_id)
    ):
        raise DispatchError("producer_authority_release_invalid")
    processing_contract = _subject_processing_contract(config, subject)
    processing_sha256 = processing_contract.get("processing_contract_sha256")
    return _producer_authority_from_processing_contract(
        subject, release_id, processing_sha256
    )


def _producer_authority_from_processing_contract(
    subject: str, release_id: str, processing_sha256: object
) -> dict[str, Any]:
    if (
        not isinstance(processing_sha256, str)
        or len(processing_sha256) != 64
        or any(char not in "0123456789abcdef" for char in processing_sha256)
    ):
        raise DispatchError("producer_processing_contract_invalid")
    core = {
        "schema_version": PRODUCER_AUTHORITY_SCHEMA,
        "subject": subject,
        "release_id": release_id,
        "loaded_core_sha256": LOADED_CORE_SHA256,
        "processing_contract_sha256": processing_sha256,
        "model": REQUIRED_MODEL,
        "reasoning_effort": REQUIRED_REASONING_EFFORT,
        "formal_write_count": 0,
    }
    return {**core, "authority_fingerprint": sha256_value(core)}


def _producer_source_events(
    member_payloads: Sequence[Mapping[str, Any]], subject: str
) -> list[dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for member in member_payloads:
        capture_id = member.get("capture_id")
        recorded_at = member.get("recorded_at")
        input_fingerprint = member.get("input_fingerprint")
        input_binding = member.get("input_binding")
        model_input = member.get("model_input")
        if (
            not isinstance(capture_id, str)
            or not capture_id
            or not isinstance(recorded_at, str)
            or not recorded_at
            or not isinstance(input_fingerprint, str)
            or not input_fingerprint
            or not isinstance(input_binding, Mapping)
            or not isinstance(model_input, Mapping)
        ):
            raise DispatchError("producer_input_member_invalid")
        member_rows: list[dict[str, str]] = []
        batch_events = model_input.get("batch_events")
        capture_hashes = input_binding.get("capture_event_sha256")
        if subject == "english" and isinstance(batch_events, list):
            if not isinstance(capture_hashes, Mapping) or not batch_events:
                raise DispatchError("producer_english_event_binding_invalid")
            for event in batch_events:
                if not isinstance(event, Mapping):
                    raise DispatchError("producer_english_event_invalid")
                event_id = event.get("event_id")
                occurred_at = event.get("occurred_at")
                source_sha256 = (
                    capture_hashes.get(event_id)
                    if isinstance(event_id, str)
                    else None
                )
                if (
                    not isinstance(event_id, str)
                    or not event_id
                    or not isinstance(occurred_at, str)
                    or not occurred_at
                    or not isinstance(source_sha256, str)
                    or source_sha256 != sha256_value(event)
                ):
                    raise DispatchError("producer_english_event_binding_invalid")
                member_rows.append(
                    {
                        "event_id": event_id,
                        "recorded_at": occurred_at,
                        "source_sha256": source_sha256,
                    }
                )
        else:
            source_value = {
                "subject": subject,
                "capture_id": capture_id,
                "recorded_at": recorded_at,
                "input_fingerprint": input_fingerprint,
                "input_binding": copy.deepcopy(dict(input_binding)),
                "model_input": copy.deepcopy(dict(model_input)),
            }
            member_rows.append(
                {
                    "event_id": capture_id,
                    "recorded_at": recorded_at,
                    "source_sha256": sha256_value(source_value),
                }
            )
        for row in member_rows:
            previous = rows.get(row["event_id"])
            if previous is not None and previous != row:
                raise DispatchError("producer_source_event_conflict")
            rows[row["event_id"]] = row
    if not rows:
        raise DispatchError("producer_source_events_missing")
    return sorted(
        rows.values(),
        key=lambda row: (
            row["recorded_at"],
            row["event_id"],
            row["source_sha256"],
        ),
    )


def producer_dispatch_input_contract(
    *,
    config: Mapping[str, Any],
    subject: str,
    release_id: str,
    producer_unit_id: str,
    producer_recorded_at: str,
    input_fingerprint: str,
    member_payloads: Sequence[Mapping[str, Any]],
    processing_contract_sha256: str,
) -> dict[str, Any]:
    del config
    authority = _producer_authority_from_processing_contract(
        subject, release_id, processing_contract_sha256
    )
    if (
        not isinstance(producer_unit_id, str)
        or not producer_unit_id
        or not isinstance(producer_recorded_at, str)
        or not producer_recorded_at
        or not isinstance(input_fingerprint, str)
        or not input_fingerprint
    ):
        raise DispatchError("producer_input_contract_invalid")
    source_events = _producer_source_events(member_payloads, subject)
    capture_type: str | None = None
    evidence_contract: dict[str, Any] | None = None
    evidence_contract_sha256: str | None = None
    if subject == "math":
        contracts: list[tuple[str, dict[str, Any], str]] = []
        for payload in member_payloads:
            binding = payload.get("input_binding")
            if not isinstance(binding, Mapping):
                raise DispatchError("math_evidence_contract_missing")
            member_type = binding.get("capture_type")
            member_contract = binding.get("math_evidence_contract")
            member_sha256 = binding.get("math_evidence_contract_sha256")
            if (
                member_type
                not in {
                    "formal_problem",
                    "new_source_problem",
                    "existing_formal_card_observation",
                    "fact_observation",
                }
                or not isinstance(member_contract, Mapping)
                or member_contract.get("capture_type") != member_type
                or member_contract.get("evidence_status") != "ready"
                or member_contract.get("missing_roles") != []
                or member_contract.get("formal_write_count") != 0
                or member_sha256 != sha256_value(member_contract)
            ):
                raise DispatchError("math_evidence_contract_invalid")
            contracts.append(
                (
                    str(member_type),
                    copy.deepcopy(dict(member_contract)),
                    str(member_sha256),
                )
            )
        if any(row != contracts[0] for row in contracts[1:]):
            raise DispatchError("math_evidence_contract_group_mismatch")
        capture_type, evidence_contract, evidence_contract_sha256 = contracts[0]
    core = {
        "schema_version": PRODUCER_DISPATCH_INPUT_SCHEMA,
        "subject": subject,
        "release_id": release_id,
        "authority_fingerprint": authority["authority_fingerprint"],
        "producer_unit_id": producer_unit_id,
        "producer_recorded_at": producer_recorded_at,
        "input_fingerprint": input_fingerprint,
        "source_events": source_events,
        "earliest_source_recorded_at": min(
            row["recorded_at"] for row in source_events
        ),
        "latest_source_recorded_at": max(
            row["recorded_at"] for row in source_events
        ),
        "source_event_set_sha256": sha256_value(source_events),
        "capture_type": capture_type,
        "evidence_contract": evidence_contract,
        "evidence_contract_sha256": evidence_contract_sha256,
        "formal_write_count": 0,
    }
    return {**core, "producer_input_contract_sha256": sha256_value(core)}


def _release_subject_processing_contract(
    runtime_root: Path, release_id: str, subject: str
) -> dict[str, Any]:
    key = (str(runtime_root.resolve()), release_id, subject)
    cached = _RELEASE_SUBJECT_CONTRACT_CACHE.get(key)
    if cached is not None:
        return copy.deepcopy(cached)
    release_root = (runtime_root / "releases" / release_id).resolve()
    if release_root.parent != (runtime_root / "releases").resolve():
        raise DispatchError("semantic_reuse_release_path_invalid")
    config_path = release_root / "config.json"
    core_path = release_root / "lib" / "preprocessor_core.py"
    if not config_path.is_file() or not core_path.is_file():
        raise DispatchError("semantic_reuse_source_release_missing")
    script = (
        "import json,sys; from pathlib import Path; "
        "sys.path.insert(0, sys.argv[1]); import preprocessor_core as p; "
        "cfg=p.load_config(Path(sys.argv[2])); "
        "fn={'math':p.math_processing_contract,'cs408':p.cs408_processing_contract,"
        "'english':p.english_processing_contract}[sys.argv[3]]; "
        "print(json.dumps(fn(cfg),ensure_ascii=False,sort_keys=True))"
    )
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                str(release_root / "lib"),
                str(config_path),
                subject,
            ],
            cwd=release_root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DispatchError("semantic_reuse_contract_probe_failed") from exc
    if completed.returncode != 0:
        raise DispatchError("semantic_reuse_contract_probe_failed")
    try:
        value = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise DispatchError("semantic_reuse_contract_probe_invalid") from exc
    if not isinstance(value, dict):
        raise DispatchError("semantic_reuse_contract_probe_invalid")
    _RELEASE_SUBJECT_CONTRACT_CACHE[key] = copy.deepcopy(value)
    return value


def _normalized_subject_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(dict(contract))
    for key in (
        "loaded_core_sha256",
        "processing_contract_sha256",
        "semantic_contract_schema_version",
        "subject_semantic_code_closure_sha256",
        "subject_dispatch_bridge_code_closure_sha256",
    ):
        normalized.pop(key, None)
    return normalized


def _semantic_evidence_capsule(
    *,
    subject: str,
    model_input: Mapping[str, Any],
    allowed_evidence_refs: list[Any] | tuple[Any, ...],
    ordered_image_sha256s: list[str],
    input_binding: Mapping[str, Any],
    canonical_state: Any,
    sol_state: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "study-intake-semantic-evidence-capsule-v1",
        "subject": subject,
        "model_input": copy.deepcopy(dict(model_input)),
        "allowed_evidence_refs": list(allowed_evidence_refs),
        "ordered_image_sha256s": list(ordered_image_sha256s),
        "candidate_route": {
            "candidate_kind": input_binding.get("candidate_kind"),
            "source_kind": input_binding.get("source_kind"),
            "source_route": input_binding.get("source_route"),
            "canonical_state": canonical_state,
            "sol_state": sol_state,
        },
    }


def _source_member_payload(
    package: Mapping[str, Any], capture_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    task = package.get("task")
    frozen = task.get("frozen_payload") if isinstance(task, Mapping) else None
    if not isinstance(frozen, Mapping):
        raise DispatchError("semantic_reuse_source_task_invalid")
    members = frozen.get("content_group_members")
    member: Mapping[str, Any] | None = None
    if isinstance(members, list):
        member = next(
            (
                row
                for row in members
                if isinstance(row, Mapping)
                and row.get("capture_id") == capture_id
            ),
            None,
        )
    if member is None and frozen.get("capture_id") == capture_id:
        member = frozen
    if not isinstance(member, Mapping):
        raise DispatchError("semantic_reuse_source_member_missing")
    return copy.deepcopy(dict(frozen)), copy.deepcopy(dict(member))


def _try_semantic_package_reuse(
    *,
    config: Mapping[str, Any],
    candidate: Candidate,
    release_id: str,
    rule_binding: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any] | None:
    raw_runtime_root = config.get("runtime_root")
    if not isinstance(raw_runtime_root, str) or not raw_runtime_root:
        return None
    runtime_root = Path(raw_runtime_root).resolve()
    store = LeaseStore(runtime_root)
    latest_path = store._latest_path(candidate.subject, candidate.capture_id)
    try:
        if not latest_path.is_file():
            return None
        latest_raw = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DispatchError("semantic_reuse_latest_unreadable") from exc
    if not isinstance(latest_raw, Mapping):
        raise DispatchError("semantic_reuse_latest_invalid")
    source_release_id = latest_raw.get("release_id")
    if not isinstance(source_release_id, str) or len(source_release_id) != 64:
        raise DispatchError("semantic_reuse_source_release_invalid")
    verified = store.verify_authoritative_completion(
        candidate.subject,
        candidate.capture_id,
        expected_release_id=source_release_id,
    )
    completion = verified["completion"]
    package = verified.get("package")
    if completion.get("outcome") != "succeeded" or not isinstance(
        package, Mapping
    ):
        return None
    source_frozen, source_member = _source_member_payload(
        package, candidate.capture_id
    )
    source_binding = source_member.get("input_binding")
    source_model_input = source_member.get("model_input")
    if not isinstance(source_binding, Mapping) or not isinstance(
        source_model_input, Mapping
    ):
        raise DispatchError("semantic_reuse_source_candidate_invalid")
    current_capsule = _semantic_evidence_capsule(
        subject=candidate.subject,
        model_input=candidate.model_input,
        allowed_evidence_refs=candidate.allowed_evidence_refs,
        ordered_image_sha256s=list(identity["ordered_image_sha256s"]),
        input_binding=candidate.input_binding,
        canonical_state=candidate.canonical_state,
        sol_state=candidate.sol_state,
    )
    source_capsule = _semantic_evidence_capsule(
        subject=candidate.subject,
        model_input=source_model_input,
        allowed_evidence_refs=list(source_member.get("allowed_evidence_refs") or []),
        ordered_image_sha256s=list(
            source_frozen.get("content_evidence_image_sha256s") or []
        ),
        input_binding=source_binding,
        canonical_state=source_member.get("canonical_state"),
        sol_state=source_member.get("sol_state"),
    )
    if current_capsule != source_capsule:
        return None
    current_content_id = str(identity["content_processing_id"])
    source_content_id = str(completion.get("content_processing_id") or "")
    exact_identity = source_content_id == current_content_id

    def controlled_replay_required(reason: str) -> dict[str, Any]:
        return {
            "disposition": "controlled_replay_required",
            "reason": reason,
            "source_release_id": source_release_id,
            "source_package_sha256": completion.get("package_sha256"),
            "source_content_processing_id": source_content_id,
            "semantic_evidence_capsule_sha256": sha256_value(
                current_capsule
            ),
        }

    if candidate.subject == "math" and not exact_identity:
        return controlled_replay_required(
            "historical_terminal_semantic_contract_changed"
        )
    current_contract = _subject_processing_contract(config, candidate.subject)
    current_contract_sha = current_contract.get("processing_contract_sha256")
    if current_contract_sha != candidate.input_binding.get(
        "processing_contract_sha256"
    ):
        raise DispatchError("semantic_reuse_current_contract_mismatch")
    source_contract = _release_subject_processing_contract(
        runtime_root, source_release_id, candidate.subject
    )
    source_contract_sha = source_contract.get("processing_contract_sha256")
    if source_contract_sha != source_binding.get("processing_contract_sha256"):
        raise DispatchError("semantic_reuse_source_contract_mismatch")
    current_normalized = _normalized_subject_contract(current_contract)
    source_normalized = _normalized_subject_contract(source_contract)
    if current_normalized != source_normalized:
        return controlled_replay_required(
            "historical_terminal_semantic_contract_changed"
        )
    current_closure = subject_semantic_code_closure_manifest_from_path(
        candidate.subject, Path(__file__).with_name("preprocessor_core.py")
    )
    source_core_path = (
        runtime_root
        / "releases"
        / source_release_id
        / "lib"
        / "preprocessor_core.py"
    )
    source_closure = subject_semantic_code_closure_manifest_from_path(
        candidate.subject, source_core_path
    )
    if current_closure["components"] != source_closure["components"]:
        return controlled_replay_required(
            "historical_terminal_semantic_code_changed"
        )
    current_bridge_closure = (
        subject_dispatch_bridge_code_closure_manifest_from_path(
            candidate.subject, Path(__file__)
        )
    )
    current_bridge_contract_sha = current_contract.get(
        "subject_dispatch_bridge_code_closure_sha256"
    )
    if current_bridge_contract_sha != current_bridge_closure.get(
        "code_closure_sha256"
    ):
        raise DispatchError("semantic_reuse_current_bridge_closure_mismatch")
    source_bridge_contract_sha = source_contract.get(
        "subject_dispatch_bridge_code_closure_sha256"
    )
    source_bridge_verified = False
    if isinstance(source_bridge_contract_sha, str):
        source_bridge_closure = (
            subject_dispatch_bridge_code_closure_manifest_from_path(
                candidate.subject,
                runtime_root
                / "releases"
                / source_release_id
                / "lib"
                / "core_dispatch_bridge.py",
            )
        )
        if source_bridge_contract_sha != source_bridge_closure.get(
            "code_closure_sha256"
        ):
            raise DispatchError(
                "semantic_reuse_source_bridge_closure_mismatch"
            )
        if (
            current_bridge_closure["components"]
            != source_bridge_closure["components"]
        ):
            return controlled_replay_required(
                "historical_terminal_dispatch_contract_changed"
            )
        source_bridge_verified = True
    source_loaded_core = source_contract.get("loaded_core_sha256")
    if not isinstance(source_loaded_core, str):
        source_loaded_core = package.get("loaded_core_sha256")
    if not isinstance(source_loaded_core, str) or len(source_loaded_core) != 64:
        raise DispatchError("semantic_reuse_source_core_identity_missing")
    completion_path = Path(str(latest_raw.get("completion_path") or ""))
    completion_sha = latest_raw.get("completion_sha256")
    if not isinstance(completion_sha, str) or not completion_path.is_file():
        raise DispatchError("semantic_reuse_source_completion_invalid")
    receipt = verified["receipt"]
    ledger = verified["ledger_entry"]
    evidence_capsule_sha = sha256_value(current_capsule)
    normalized_contract_sha = sha256_value(current_normalized)
    reuse = store.publish_semantic_package_reuse(
        {
            "subject": candidate.subject,
            "capture_id": candidate.capture_id,
            "current_release_id": release_id,
            "current_loaded_core_sha256": LOADED_CORE_SHA256,
            "current_processing_contract_sha256": str(current_contract_sha),
            "current_code_closure_sha256": str(
                current_closure["code_closure_sha256"]
            ),
            "source_release_id": source_release_id,
            "source_loaded_core_sha256": source_loaded_core,
            "source_processing_contract_sha256": str(source_contract_sha),
            "source_code_closure_sha256": str(
                source_closure["code_closure_sha256"]
            ),
            "source_unit_sha256": str(completion["unit_sha256"]),
            "source_content_processing_id": source_content_id,
            "source_completion_sha256": completion_sha,
            "source_receipt_sha256": str(completion["receipt_sha256"]),
            "source_ledger_entry_sha256": str(
                completion["ledger_entry_sha256"]
            ),
            "source_package_sha256": str(completion["package_sha256"]),
            "semantic_evidence_capsule_sha256": evidence_capsule_sha,
            "normalized_contract_sha256": normalized_contract_sha,
            "compatibility_reason": (
                "exact_subject_semantic_identity"
                if exact_identity
                else (
                    "legacy_global_core_only_migration"
                    if source_bridge_verified
                    else "legacy_subject_bridge_closure_introduction"
                )
            ),
            "source_receipt_formal_write_count": receipt.get(
                "formal_write_count"
            ),
            "source_ledger_formal_write_count": ledger.get(
                "formal_write_count"
            ),
            "formal_write_count": 0,
            "model_call_count": 0,
        }
    )
    dispatch_contract = {
        "schema_version": "study-intake-dispatch-release-binding-v1",
        **rule_binding,
        "loaded_core_sha256": LOADED_CORE_SHA256,
        "dispatch_reason": "semantic_package_reused",
        "content_processing_id": current_content_id,
        "model_enqueue_allowed": False,
        "requested_service_tier": None,
        "fast_mode_requested": False,
        "fast_mode_effective": "not_requested",
    }
    member_payload = dict(FrozenTask.from_candidate(candidate).frozen_payload)
    payload = {
        **member_payload,
        "content_processing_id": current_content_id,
        "content_group_members": [member_payload],
        "content_group_capture_ids": [candidate.capture_id],
        "content_evidence_image_sha256s": list(
            identity["ordered_image_sha256s"]
        ),
        "dispatch_contract": dispatch_contract,
    }
    task = FrozenTask(payload)
    return {
        "task": task,
        "reuse": reuse,
        "source_package_sha256": completion["package_sha256"],
        "source_release_id": source_release_id,
        "source_content_processing_id": source_content_id,
    }


def validate_fixed_model_contract(config: Mapping[str, Any]) -> None:
    model = config.get("model")
    if not isinstance(model, Mapping):
        raise DispatchError("config_model_missing")
    if model.get("model") != REQUIRED_MODEL:
        raise DispatchError("config_model_mismatch")
    if model.get("reasoning_effort") != REQUIRED_REASONING_EFFORT:
        raise DispatchError("config_reasoning_effort_mismatch")
    dispatch = config.get("dispatch")
    canary = (
        dispatch.get("production_canary")
        if isinstance(dispatch, Mapping)
        else None
    )
    if "service_tier" in model:
        raise DispatchError("config_service_tier_mismatch")


def _ordered_image_content_sha256s(candidate: Candidate) -> list[str]:
    """Hash the ordered evidence bytes without binding work to local paths."""

    digests: list[str] = []
    for raw_path in candidate.image_paths:
        path = Path(raw_path)
        try:
            if not path.is_file():
                raise DispatchError("content_evidence_image_missing")
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
        except DispatchError:
            raise
        except OSError as exc:
            raise DispatchError("content_evidence_image_unreadable") from exc
        digests.append(digest.hexdigest())
    return digests


def _validate_candidate_image_hash_binding(
    candidate: Candidate, ordered_image_sha256s: list[str]
) -> None:
    """Fail closed when frozen image bytes differ from declared evidence."""

    if candidate.subject != "math":
        return
    source_bundle = candidate.model_input.get("source_bundle")
    artifacts = source_bundle.get("artifacts") if isinstance(
        source_bundle, Mapping
    ) else None
    if not isinstance(artifacts, list):
        raise DispatchError("candidate_image_evidence_binding_invalid")

    allowed_roles = {"question", "solution", "user_work", "reference"}
    expected: list[str] = []
    for row in artifacts:
        if not isinstance(row, Mapping):
            raise DispatchError("candidate_image_evidence_binding_invalid")
        role = row.get("role")
        provided_to_model = row.get("provided_to_model")
        if role not in allowed_roles or not isinstance(provided_to_model, bool):
            raise DispatchError("candidate_image_evidence_binding_invalid")
        if provided_to_model is not True:
            continue
        digest = row.get("sha256")
        if (
            not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            raise DispatchError("candidate_image_evidence_binding_invalid")
        # Preserve the producer-declared artifact order exactly.  Local path
        # order is part of the frozen model request and may not be regrouped by
        # role or sorted by hash at this boundary.
        expected.append(digest)
    if expected != ordered_image_sha256s:
        raise DispatchError("candidate_image_evidence_hash_mismatch")


def content_processing_identity(
    candidate: Candidate,
    rule_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the stable identity of exactly one frozen candidate task.

    Distinct capture events never share model work, even when they point at the
    same formal target or happen to contain the same semantic evidence.  The
    candidate payload digest makes the identity exact: only another submission
    of the same capture with the same complete frozen payload can deduplicate.
    """

    expected = validate_dispatch_rule_binding(
        rule_binding,
        subject=candidate.subject,
        require_processing_contract=True,
    )
    processing_contract_sha256 = candidate.input_binding.get(
        "processing_contract_sha256"
    )
    if processing_contract_sha256 != expected.get(
        "subject_processing_contract_sha256"
    ):
        raise DispatchError("candidate_processing_contract_binding_mismatch")
    ordered_image_sha256s = _ordered_image_content_sha256s(candidate)
    _validate_candidate_image_hash_binding(candidate, ordered_image_sha256s)
    candidate_payload_sha256 = FrozenTask.from_candidate(
        candidate
    ).frozen_payload_sha256
    capsule: dict[str, Any] = {
        "schema_version": "study-intake-content-processing-identity-v1",
        "subject": candidate.subject,
        "capture_id": candidate.capture_id,
        "candidate_frozen_payload_sha256": candidate_payload_sha256,
        "model_input": copy.deepcopy(candidate.model_input),
        "allowed_evidence_refs": list(candidate.allowed_evidence_refs),
        "ordered_image_sha256s": ordered_image_sha256s,
        "candidate_route": {
            "candidate_kind": candidate.input_binding.get("candidate_kind"),
            "source_kind": candidate.input_binding.get("source_kind"),
            "canonical_state": candidate.canonical_state,
            "sol_state": candidate.sol_state,
        },
        "processing_contract_sha256": processing_contract_sha256,
        "model": REQUIRED_MODEL,
        "reasoning_effort": REQUIRED_REASONING_EFFORT,
    }
    if candidate.subject == "math":
        capsule["math_group_processing_key"] = math_group_processing_key(
            candidate
        )
    canonical = json.dumps(
        capsule,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return {
        "content_processing_id": hashlib.sha256(canonical).hexdigest(),
        "ordered_image_sha256s": ordered_image_sha256s,
    }


def validate_concurrent_cs408_candidate(candidate: Candidate) -> None:
    """Require native v3 evidence or a fully bound legacy-v1 receipt."""

    if candidate.subject != "cs408":
        return
    raw_bundle = candidate.model_input.get("current_question_evidence")
    if not isinstance(raw_bundle, Mapping):
        raise DispatchError("concurrent_cs408_evidence_missing")
    bundle_schema = raw_bundle.get("schema_version")
    if bundle_schema not in {
        CURRENT_QUESTION_BUNDLE_SCHEMA,
        CURRENT_QUESTION_BUNDLE_SCHEMA_V3,
    }:
        raise DispatchError("concurrent_cs408_evidence_v3_required")
    raw_receipt = candidate.model_input.get(
        "legacy_current_question_compatibility"
    )
    if (
        bundle_schema == CURRENT_QUESTION_BUNDLE_SCHEMA
        and not isinstance(raw_receipt, Mapping)
    ):
        raise DispatchError(
            "concurrent_cs408_legacy_resolution_required"
        )
    source_id = raw_bundle.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        raise DispatchError("concurrent_cs408_evidence_invalid")
    try:
        bundle = validate_current_question_bundle(
            raw_bundle,
            study_date=candidate.study_date,
            source_id=source_id,
        )
    except PreprocessorError as exc:
        raise DispatchError("concurrent_cs408_evidence_invalid") from exc
    if current_question_bundle_evidence_status(bundle) != "ready":
        raise DispatchError("concurrent_cs408_evidence_not_ready")

    if bundle_schema == CURRENT_QUESTION_BUNDLE_SCHEMA_V3:
        raw_trace = raw_bundle.get("interaction_trace")
        if (
            not isinstance(raw_trace, Mapping)
            or raw_trace.get("schema") != INTERACTION_TRACE_SCHEMA_V2
        ):
            raise DispatchError("concurrent_cs408_trace_v2_required")
        if candidate.input_binding.get("evidence_status") != "ready":
            raise DispatchError("concurrent_cs408_evidence_not_ready")
        return

    if candidate.image_paths:
        raise DispatchError(
            "concurrent_cs408_legacy_image_evidence_unsupported"
        )
    assert isinstance(raw_receipt, Mapping)
    receipt = dict(raw_receipt)
    receipt_sha256 = receipt.pop("compatibility_receipt_sha256", None)
    if (
        not isinstance(receipt_sha256, str)
        or len(receipt_sha256) != 64
        or sha256_value(receipt) != receipt_sha256
    ):
        raise DispatchError(
            "concurrent_cs408_legacy_compatibility_invalid"
        )
    binding = candidate.input_binding
    expected = {
        "schema_version": LEGACY_CURRENT_QUESTION_COMPATIBILITY_SCHEMA,
        "capture_id": candidate.capture_id,
        "study_date": candidate.study_date,
        "evidence_manifest_sha256": binding.get(
            "evidence_manifest_sha256"
        ),
        "evidence_bundle_sha256": binding.get("evidence_bundle_sha256"),
        "evidence_bundle_schema_version": CURRENT_QUESTION_BUNDLE_SCHEMA,
        "interaction_trace_binding_sha256": binding.get(
            "interaction_trace_binding_sha256"
        ),
        "interaction_trace_object_sha256": binding.get(
            "interaction_trace_object_sha256"
        ),
        "interaction_trace_payload_sha256": binding.get(
            "interaction_trace_payload_sha256"
        ),
        "resolution_receipt_sha256": binding.get(
            "interaction_trace_resolution_receipt_sha256"
        ),
        "background_handoff_binding_sha256": binding.get(
            "background_handoff_binding_sha256"
        ),
        "background_handoff_object_sha256": binding.get(
            "background_handoff_object_sha256"
        ),
        "formal_write_count": 0,
    }
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise DispatchError(
            "concurrent_cs408_legacy_compatibility_binding_mismatch"
        )
    if (
        receipt.get("interaction_trace_supplement_kind")
        != "resolved_trace"
        or receipt.get("background_handoff_status") != "ready"
        or receipt.get("background_handoff_completion_kind")
        != "teaching_resolved"
        or receipt.get("normalized_result")
        != "failure_standing"
        or receipt.get("question_mode") != "dialogue_only"
    ):
        raise DispatchError(
            "concurrent_cs408_legacy_compatibility_invalid"
        )
    if binding.get("legacy_compatibility_receipt_sha256") != receipt_sha256:
        raise DispatchError(
            "concurrent_cs408_legacy_compatibility_binding_mismatch"
        )


def scan_eligible_candidates(
    config: Mapping[str, Any],
    subject: str,
    *,
    worker_factory: Callable[[Mapping[str, Any]], Worker] = Worker,
    capture_allowlist: frozenset[str] | None = None,
    candidate_overrides: Sequence[Candidate] | None = None,
    controlled_replay: bool = False,
    publish_evidence_readiness: bool = True,
    producer_recorded_after: str | None = None,
) -> tuple[list[EligibleFrozenCandidate], list[dict[str, Any]]]:
    """Freeze every currently eligible candidate for exactly one subject."""

    validate_fixed_model_contract(config)
    if capture_allowlist is not None and (
        not capture_allowlist
        or any(not isinstance(item, str) or not item for item in capture_allowlist)
    ):
        raise DispatchError("capture_allowlist_invalid")
    if controlled_replay and capture_allowlist is None:
        raise DispatchError("controlled_replay_allowlist_required")
    if candidate_overrides is not None and (
        not candidate_overrides
        or any(
            not isinstance(candidate, Candidate)
            or candidate.subject != subject
            or (
                capture_allowlist is not None
                and candidate.capture_id not in capture_allowlist
            )
            for candidate in candidate_overrides
        )
    ):
        raise DispatchError("candidate_overrides_invalid")
    scanner = worker_factory(config)
    release_id = getattr(scanner, "release_id", None)
    if not isinstance(release_id, str) or not release_id:
        raise DispatchError("scanner_release_id_missing")
    if producer_recorded_after is not None:
        if subject != "math" or controlled_replay:
            raise DispatchError("producer_discovery_floor_invalid")
        discovery_floor = parse_time(producer_recorded_after)
        if discovery_floor is None:
            raise DispatchError("producer_discovery_floor_invalid")
        statuses = scanner.scan_statuses(None, only_subject="math")
        math_status = statuses.get("math")
        pending = (
            math_status.get("pending")
            if isinstance(math_status, Mapping)
            else None
        )
        if not isinstance(pending, list):
            raise DispatchError("producer_discovery_floor_status_invalid")
        discovered = frozenset(
            str(row["event_id"])
            for row in pending
            if isinstance(row, Mapping)
            and isinstance(row.get("event_id"), str)
            and parse_time(row.get("recorded_at")) is not None
            and parse_time(row.get("recorded_at")) > discovery_floor
        )
        capture_allowlist = (
            discovered
            if capture_allowlist is None
            else frozenset(capture_allowlist & discovered)
        )
    study_date = current_date(str(config["timezone"]))
    selector_study_date = study_date
    selector_capture_allowlist = capture_allowlist
    if (
        subject == "math"
        and capture_allowlist is None
        and candidate_overrides is None
        and not controlled_replay
    ):
        raw_runtime_root = config.get("runtime_root")
        raw_math_adapter = (
            config.get("adapters", {}).get("math")
            if isinstance(config.get("adapters"), Mapping)
            else None
        )
        raw_math_repo_root = (
            raw_math_adapter.get("repo_root")
            if isinstance(raw_math_adapter, Mapping)
            else None
        )
        try:
            exact_scopes = (
                pending_exact_smoke_dispatch_scope(
                    runtime_root=Path(raw_runtime_root),
                    math_repo_root=Path(raw_math_repo_root),
                    release_id=release_id,
                )
                if isinstance(raw_runtime_root, str)
                and Path(raw_runtime_root).is_absolute()
                and isinstance(raw_math_repo_root, str)
                and Path(raw_math_repo_root).is_absolute()
                else None
            )
        except MathExactSmokeError as exc:
            raise DispatchError(exc.code) from exc
        if exact_scopes:
            exact_dates = {scope.get("study_date") for scope in exact_scopes}
            exact_capture_ids = [
                scope.get("capture_event_id") for scope in exact_scopes
            ]
            if (
                len(exact_dates) != 1
                or not all(
                    isinstance(exact_date, str) and exact_date
                    for exact_date in exact_dates
                )
                or not all(
                    isinstance(capture_id, str) and capture_id
                    for capture_id in exact_capture_ids
                )
                or len(set(exact_capture_ids)) != len(exact_capture_ids)
            ):
                raise DispatchError("math_smoke_pending_dispatch_scope_invalid")
            selector_study_date = str(next(iter(exact_dates)))
            selector_capture_allowlist = frozenset(exact_capture_ids)
    if candidate_overrides is not None:
        rows = [
            (candidate, "controlled_replay_external_input")
            for candidate in candidate_overrides
        ]
    else:
        selector = scanner.eligible_candidates
        parameters = inspect.signature(selector).parameters
        accepts_keywords = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        optional = {
            # Producer study dates describe when learning happened, not when
            # an immutable Capture was appended.  The production-canary
            # watermark and the durable job/queue identities already fence
            # historical and consumed inputs, so discovery must span dates
            # for every subject or a delayed intake is silently invisible.
            "scan_all_study_dates": subject in {"math", "cs408", "english"},
            "capture_allowlist": selector_capture_allowlist,
            "controlled_replay": controlled_replay,
        }
        selector_kwargs = {"mutate_recovery_state": False}
        selector_kwargs.update(
            {
                name: value
                for name, value in optional.items()
                if accepts_keywords or name in parameters
            }
        )
        rows = selector(subject, selector_study_date, **selector_kwargs)
    if selector_capture_allowlist is not None:
        rows = [
            row
            for row in rows
            if (
                row[0].capture_id in selector_capture_allowlist
                or (
                    row[0].subject == "english"
                    and isinstance(
                        row[0].input_binding.get("capture_event_ids"), list
                    )
                    and set(row[0].input_binding["capture_event_ids"])
                    <= selector_capture_allowlist
                )
            )
        ]
    route_rejections: list[tuple[Candidate, str]] = []
    adapter = getattr(scanner, "adapters", {}).get(subject)
    raw_diagnostics = getattr(adapter, "candidate_diagnostics", {})
    raw_errors = getattr(adapter, "candidate_errors", {})
    raw_error_context = getattr(adapter, "candidate_error_context", {})
    diagnostics = (
        raw_diagnostics if isinstance(raw_diagnostics, Mapping) else {}
    )
    candidate_errors = raw_errors if isinstance(raw_errors, Mapping) else {}
    error_context = (
        raw_error_context if isinstance(raw_error_context, Mapping) else {}
    )
    readiness_by_capture: dict[str, dict[str, Any]] = {}
    if subject == "cs408" and diagnostics and publish_evidence_readiness:
        readiness_store = LeaseStore(Path(str(config["runtime_root"])))
        for capture_id, diagnostic in diagnostics.items():
            if (
                capture_allowlist is not None
                and capture_id not in capture_allowlist
            ):
                continue
            if not isinstance(capture_id, str) or not isinstance(
                diagnostic, Mapping
            ):
                continue
            try:
                readiness_by_capture[capture_id] = (
                    readiness_store.publish_evidence_readiness(
                        diagnostic, expected_release_id=release_id
                    )
                )
            except (DispatchError, OSError) as exc:
                candidate_errors = dict(candidate_errors)
                candidate_errors[capture_id] = (
                    exc.code
                    if isinstance(exc, DispatchError)
                    else "evidence_readiness_publish_failed"
                )
    frozen: list[EligibleFrozenCandidate] = []
    decisions: list[dict[str, Any]] = []
    prepared_groups: dict[
        str,
        list[tuple[Candidate, str, dict[str, Any], dict[str, Any]]],
    ] = {}
    accepted_rows: list[tuple[Candidate, str]] = []
    reused_capture_ids: set[str] = set()
    for candidate, reason in rows:
        try:
            if candidate.subject != subject:
                raise DispatchError("candidate_subject_mismatch")
            candidate = bind_candidate_producer_attestations(
                config, candidate
            )
            validate_concurrent_cs408_candidate(candidate)
            processing_contract_sha256 = candidate.input_binding.get(
                "processing_contract_sha256"
            )
            if not isinstance(processing_contract_sha256, str):
                raise DispatchError(
                    "candidate_processing_contract_sha256_missing"
                )
            rule_binding = dispatch_rule_binding(
                release_id=release_id,
                subject=candidate.subject,
                subject_processing_contract_sha256=(
                    processing_contract_sha256
                ),
            )
            identity = content_processing_identity(candidate, rule_binding)
            reuse = (
                None
                if controlled_replay
                else _try_semantic_package_reuse(
                    config=config,
                    candidate=candidate,
                    release_id=release_id,
                    rule_binding=rule_binding,
                    identity=identity,
                )
            )
        except (DispatchError, PreprocessorError) as exc:
            route_rejections.append((candidate, exc.code))
            continue
        except SkillBindingError as exc:
            route_rejections.append((candidate, exc.code))
            continue
        except (OSError, TypeError, ValueError):
            route_rejections.append(
                (candidate, "candidate_content_freeze_failed")
            )
            continue
        if reuse is not None:
            if reuse.get("disposition") == "controlled_replay_required":
                decisions.append(
                    {
                        "subject": candidate.subject,
                        "capture_id": candidate.capture_id,
                        "study_date": candidate.study_date,
                        "target_label": candidate.target_label,
                        "input_fingerprint": candidate.input_fingerprint,
                        "eligible": False,
                        "reason": reuse["reason"],
                        "content_processing_id": identity[
                            "content_processing_id"
                        ],
                        "release_id": release_id,
                        "rule_version": rule_binding["rule_version"],
                        "rule_version_sha256": rule_binding[
                            "rule_version_sha256"
                        ],
                        "subject_processing_contract_sha256": rule_binding[
                            "subject_processing_contract_sha256"
                        ],
                        "phase": "historical_terminal",
                        "model": REQUIRED_MODEL,
                        "reasoning_effort": REQUIRED_REASONING_EFFORT,
                        "model_enqueue_allowed": False,
                        "controlled_replay_required": True,
                        "semantic_evidence_capsule_sha256": reuse[
                            "semantic_evidence_capsule_sha256"
                        ],
                        "source_release_id": reuse["source_release_id"],
                        "source_package_sha256": reuse[
                            "source_package_sha256"
                        ],
                        "formal_write_count": 0,
                    }
                )
                continue
            reuse_task = reuse["task"]
            reuse_receipt = reuse["reuse"]
            reused_capture_ids.add(candidate.capture_id)
            decisions.append(
                {
                    "subject": candidate.subject,
                    "capture_id": candidate.capture_id,
                    "study_date": candidate.study_date,
                    "target_label": candidate.target_label,
                    "input_fingerprint": candidate.input_fingerprint,
                    "eligible": False,
                    "reason": "semantic_package_reused",
                    "unit_sha256": reuse_task.unit_sha256,
                    "frozen_payload_sha256": (
                        reuse_task.frozen_payload_sha256
                    ),
                    "content_processing_id": identity[
                        "content_processing_id"
                    ],
                    "release_id": release_id,
                    "rule_version": rule_binding["rule_version"],
                    "rule_version_sha256": rule_binding[
                        "rule_version_sha256"
                    ],
                    "subject_processing_contract_sha256": (
                        rule_binding[
                            "subject_processing_contract_sha256"
                        ]
                    ),
                    "phase": "published",
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "model_enqueue_allowed": False,
                    "semantic_reuse_receipt_sha256": reuse_receipt[
                        "reuse_receipt_sha256"
                    ],
                    "semantic_reuse_source_release_id": reuse[
                        "source_release_id"
                    ],
                    "package_sha256": reuse["source_package_sha256"],
                    "formal_write_count": 0,
                }
            )
            continue
        content_id = str(identity["content_processing_id"])
        prepared_groups.setdefault(content_id, []).append(
            (candidate, reason, rule_binding, identity)
        )
        accepted_rows.append((candidate, reason))
    rows = accepted_rows

    for content_id, prepared_rows in prepared_groups.items():
        try:
            candidate, reason, rule_binding, identity = prepared_rows[0]
            group_rows = [
                (member, member_reason)
                for member, member_reason, _rule, _identity in prepared_rows
            ]
            member_payloads = [
                dict(FrozenTask.from_candidate(member).frozen_payload)
                for member, _member_reason in group_rows
            ]
            if len({row["capture_id"] for row in member_payloads}) != len(
                member_payloads
            ):
                raise DispatchError("content_group_capture_duplicate")
            base_task = FrozenTask(member_payloads[0])
            payload = dict(base_task.frozen_payload)
            dispatch_contract = {
                "schema_version": (
                    "study-intake-dispatch-release-binding-v1"
                ),
                **rule_binding,
                "loaded_core_sha256": LOADED_CORE_SHA256,
                "dispatch_reason": reason,
                "content_processing_id": content_id,
                "requested_service_tier": None,
                "fast_mode_requested": False,
                "fast_mode_effective": "not_requested",
            }
            dispatch_contract["producer_input_contract"] = (
                producer_dispatch_input_contract(
                    config=config,
                    subject=subject,
                    release_id=release_id,
                    producer_unit_id=candidate.capture_id,
                    producer_recorded_at=candidate.recorded_at,
                    input_fingerprint=candidate.input_fingerprint,
                    member_payloads=member_payloads,
                    processing_contract_sha256=str(
                        rule_binding[
                            "subject_processing_contract_sha256"
                        ]
                    ),
                )
            )
            if subject == "math":
                dispatch_contract.update(
                    {
                        "math_group_processing_key": (
                            math_group_processing_key(candidate)
                        ),
                        "math_group_capture_ids": [
                            member.capture_id for member, _ in group_rows
                        ],
                    }
                )
                payload["math_group_members"] = member_payloads
            # Readiness publication is a mutable scanner-side projection of
            # the same immutable producer evidence.  Keep its authenticated
            # refs on decisions, but never fold them into FrozenTask identity:
            # activation classification deliberately does not publish them,
            # while the normal daemon scan does.  The producer contract and
            # frozen evidence remain exact in both paths.
            payload.update(
                {
                    "content_processing_id": content_id,
                    "content_group_members": member_payloads,
                    "content_group_capture_ids": [
                        member.capture_id for member, _ in group_rows
                    ],
                    "content_evidence_image_sha256s": identity[
                        "ordered_image_sha256s"
                    ],
                    "dispatch_contract": dispatch_contract,
                }
            )
            task = FrozenTask(payload)
            if subject == "math":
                try:
                    raw_math = (
                        config.get("adapters", {}).get("math", {})
                        if isinstance(config.get("adapters"), Mapping)
                        else {}
                    )
                    math_repo_root = Path(
                        str(
                            raw_math.get("repo_root")
                            if isinstance(raw_math, Mapping)
                            else ""
                        )
                    )
                    exact_binding = exact_task_binding_for_capture(
                        runtime_root_loader=lambda: Path(
                            config["runtime_root"]
                        ),
                        math_repo_root=math_repo_root,
                        capture_event_id=candidate.capture_id,
                        release_id=release_id,
                        unit_sha256=task.unit_sha256,
                        content_processing_id=content_id,
                    )
                except MathExactSmokeError as exc:
                    raise DispatchError(exc.code) from exc
                if exact_binding is not None:
                    if len(group_rows) != 1:
                        raise DispatchError(
                            "math_smoke_content_group_not_singleton"
                        )
                    payload["math_exact_smoke_binding"] = exact_binding
                    rebound = FrozenTask(payload)
                    if rebound.unit_sha256 != task.unit_sha256:
                        raise DispatchError("math_smoke_task_unit_drift")
                    task = rebound
            frozen.append(EligibleFrozenCandidate(task, candidate, reason))
        except (DispatchError, PreprocessorError) as exc:
            route_rejections.extend(
                (member, exc.code)
                for member, _reason, _rule, _identity in prepared_rows
            )
            continue
        except (OSError, TypeError, ValueError):
            route_rejections.extend(
                (member, "candidate_freeze_failed")
                for member, _reason, _rule, _identity in prepared_rows
            )
            continue
        for member_index, (member, member_reason) in enumerate(group_rows):
            member_readiness = readiness_by_capture.get(member.capture_id)
            scanner_store = getattr(scanner, "store", None)
            previous_job = (
                scanner_store.read_job(member.subject, member.capture_id)
                if scanner_store is not None
                else None
            )
            previous_fingerprint = (
                previous_job.get("input_fingerprint")
                if isinstance(previous_job, Mapping)
                and isinstance(previous_job.get("input_fingerprint"), str)
                and previous_job.get("input_fingerprint")
                != member.input_fingerprint
                else None
            )
            decisions.append(
                {
                    "subject": member.subject,
                    "capture_id": member.capture_id,
                    "study_date": member.study_date,
                    "target_label": member.target_label,
                    "input_fingerprint": member.input_fingerprint,
                    "eligible": True,
                    "reason": member_reason,
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "content_processing_id": content_id,
                    "release_id": release_id,
                    "rule_version": rule_binding["rule_version"],
                    "rule_version_sha256": rule_binding[
                        "rule_version_sha256"
                    ],
                    "subject_processing_contract_sha256": (
                        rule_binding[
                            "subject_processing_contract_sha256"
                        ]
                    ),
                    "phase": "frozen_evidence",
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "model_enqueue_allowed": member_index == 0,
                    "reuses_group_owner": member_index != 0,
                    "group_owner_capture_id": candidate.capture_id,
                    "evidence_readiness_receipt_sha256": (
                        member_readiness.get("authority_receipt_sha256")
                        if member_readiness is not None
                        else None
                    ),
                    "evidence_manifest_sha256": member.input_binding.get(
                        "evidence_manifest_sha256"
                    ),
                    "evidence_bundle_sha256": member.input_binding.get(
                        "evidence_bundle_sha256"
                    ),
                    "evidence_status": member.input_binding.get(
                        "evidence_status"
                    ),
                    "legacy_compatibility_receipt_sha256": (
                        member.input_binding.get(
                            "legacy_compatibility_receipt_sha256"
                        )
                    ),
                    "superseded_input_fingerprint": previous_fingerprint,
                }
            )

    for candidate, rejection_code in route_rejections:
        rejected_rule_binding = dispatch_rule_binding(
            release_id=release_id,
            subject=subject,
            subject_processing_contract_sha256=(
                candidate.input_binding.get("processing_contract_sha256")
                if isinstance(
                    candidate.input_binding.get(
                        "processing_contract_sha256"
                    ),
                    str,
                )
                else None
            ),
        )
        rejected_task = FrozenTask(
            {
                "subject": subject,
                "capture_id": candidate.capture_id,
                "study_date": candidate.study_date,
                "target_label": candidate.target_label,
                "input_fingerprint": candidate.input_fingerprint,
                "input_binding": {"reason_code": rejection_code},
                "model_input": {},
                "allowed_evidence_refs": [],
                "image_paths": [],
                "dispatch_contract": {
                    "schema_version": (
                        "study-intake-dispatch-release-binding-v1"
                    ),
                    **rejected_rule_binding,
                    "dispatch_reason": rejection_code,
                    "model_enqueue_allowed": False,
                },
            }
        )
        decisions.append(
            {
                "subject": subject,
                "capture_id": candidate.capture_id,
                "study_date": candidate.study_date,
                "target_label": candidate.target_label,
                "input_fingerprint": candidate.input_fingerprint,
                "eligible": False,
                "reason": rejection_code,
                "error_code": rejection_code,
                "unit_sha256": rejected_task.unit_sha256,
                "frozen_payload_sha256": (
                    rejected_task.frozen_payload_sha256
                ),
                "release_id": release_id,
                "rule_version": rejected_rule_binding["rule_version"],
                "rule_version_sha256": rejected_rule_binding[
                    "rule_version_sha256"
                ],
                "subject_processing_contract_sha256": (
                    rejected_rule_binding[
                        "subject_processing_contract_sha256"
                    ]
                ),
                "phase": "frozen_evidence",
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
                "model_enqueue_allowed": False,
                "evidence_status": "rejected",
            }
        )
    handled_capture_ids = {
        candidate.capture_id for candidate, _ in rows
    } | {
        candidate.capture_id for candidate, _ in route_rejections
    } | reused_capture_ids
    for capture_id, raw_reason in candidate_errors.items():
        if (
            not isinstance(capture_id, str)
            or not isinstance(raw_reason, str)
            or capture_id in handled_capture_ids
        ):
            continue
        readiness = readiness_by_capture.get(capture_id)
        diagnostic = diagnostics.get(capture_id)
        capture_context = error_context.get(capture_id)
        capture_study_date = (
            capture_context.get("study_date")
            if isinstance(capture_context, Mapping)
            and isinstance(capture_context.get("study_date"), str)
            else study_date
        )
        deterministic_pending_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "subject": subject,
                    "capture_id": capture_id,
                    "study_date": capture_study_date,
                    "reason": raw_reason,
                    "evidence_bundle_sha256": (
                        diagnostic.get("evidence_bundle_sha256")
                        if isinstance(diagnostic, Mapping)
                        else None
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        pending_rule_binding = dispatch_rule_binding(
            release_id=release_id,
            subject=subject,
            subject_processing_contract_sha256=None,
        )
        pending_task = FrozenTask(
            {
                "subject": subject,
                "capture_id": capture_id,
                "study_date": capture_study_date,
                "target_label": (
                    diagnostic.get("target_label")
                    if isinstance(diagnostic, Mapping)
                    and isinstance(diagnostic.get("target_label"), str)
                    else capture_id
                ),
                "input_fingerprint": (
                    diagnostic.get("evidence_bundle_sha256")
                    if isinstance(diagnostic, Mapping)
                    and isinstance(diagnostic.get("evidence_bundle_sha256"), str)
                    else deterministic_pending_fingerprint
                ),
                "input_binding": {
                    "evidence_manifest_sha256": (
                        diagnostic.get("evidence_manifest_sha256")
                        if isinstance(diagnostic, Mapping)
                        else (
                            capture_context.get(
                                "evidence_manifest_sha256"
                            )
                            if isinstance(capture_context, Mapping)
                            else None
                        )
                    ),
                    "evidence_bundle_sha256": (
                        diagnostic.get("evidence_bundle_sha256")
                        if isinstance(diagnostic, Mapping)
                        else None
                    ),
                    "evidence_readiness_receipt_sha256": (
                        readiness.get("authority_receipt_sha256")
                        if readiness is not None
                        else None
                    ),
                    "reason_code": raw_reason,
                },
                "model_input": {},
                "allowed_evidence_refs": [],
                "image_paths": [],
                "dispatch_contract": {
                    "schema_version": (
                        "study-intake-dispatch-release-binding-v1"
                    ),
                    **pending_rule_binding,
                    "dispatch_reason": raw_reason,
                    "model_enqueue_allowed": False,
                },
            }
        )
        decisions.append(
            {
                "subject": subject,
                "capture_id": capture_id,
                "study_date": capture_study_date,
                "input_fingerprint": (
                    diagnostic.get("evidence_bundle_sha256")
                    if isinstance(diagnostic, Mapping)
                    else None
                ),
                "eligible": False,
                "reason": raw_reason,
                "error_code": raw_reason,
                "unit_sha256": pending_task.unit_sha256,
                "frozen_payload_sha256": pending_task.frozen_payload_sha256,
                "release_id": release_id,
                "rule_version": pending_rule_binding["rule_version"],
                "rule_version_sha256": pending_rule_binding[
                    "rule_version_sha256"
                ],
                "subject_processing_contract_sha256": None,
                "phase": "frozen_evidence",
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
                "model_enqueue_allowed": False,
                "evidence_status": (
                    diagnostic.get("status")
                    if isinstance(diagnostic, Mapping)
                    else None
                ),
                "evidence_manifest_sha256": (
                    diagnostic.get("evidence_manifest_sha256")
                    if isinstance(diagnostic, Mapping)
                    else (
                        capture_context.get("evidence_manifest_sha256")
                        if isinstance(capture_context, Mapping)
                        else None
                    )
                ),
                "evidence_bundle_sha256": (
                    diagnostic.get("evidence_bundle_sha256")
                    if isinstance(diagnostic, Mapping)
                    else None
                ),
                "evidence_readiness_receipt_sha256": (
                    readiness.get("authority_receipt_sha256")
                    if readiness is not None
                    else None
                ),
            }
        )
    return frozen, decisions


class _CapturingRunner:
    """Transparent per-task wrapper retaining only this Worker's results."""

    def __init__(self, inner: object) -> None:
        self.inner = inner
        self.results: list[ModelResult] = []

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)

    def run(self, candidate: Candidate) -> ModelResult:
        result = self.inner.run(candidate)
        self.results.append(result)
        return result

    def run_math_v2(self, candidate: Candidate) -> ModelResult:
        result = self.inner.run_math_v2(candidate)
        self.results.append(result)
        return result

    def resume_critical(
        self,
        candidate: Candidate,
        *,
        draft_analysis: Mapping[str, Any],
        analysis_receipt: Mapping[str, Any],
    ) -> ModelResult:
        resume = getattr(self.inner, "resume_critical", None)
        if not callable(resume):
            raise PreprocessorError("critical_resume_runner_unsupported")
        result = resume(
            candidate,
            draft_analysis=draft_analysis,
            analysis_receipt=analysis_receipt,
        )
        self.results.append(result)
        return result


class _DirectStageEventRecorder:
    _EVENTS = {
        "math_analysis": ("analysis_submitted", None),
        "math_critical_review": ("critical_started", "critical_completed"),
    }

    def __init__(
        self,
        runner: object,
        *,
        task: FrozenTask,
        context: TaskExecutionContext,
        store: LeaseStore,
    ) -> None:
        execute = getattr(runner, "_execute_prompt", None)
        if not callable(execute):
            raise DispatchError("direct_candidate_stage_event_runner_invalid")
        self._execute = execute
        self.task = task
        self.context = context
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        events = self._EVENTS.get(str(kwargs.get("stage_name") or ""))
        if events is None or not self.store.is_current(self.context.lease):
            raise DispatchError("direct_candidate_stage_event_invalid")
        self.store.record_task_event(self.task, self.context.lease, events[0])
        result = self._execute(*args, **kwargs)
        if events[1] is not None:
            self.store.record_task_event(self.task, self.context.lease, events[1])
        return result


class _DirectCheckpointEventRecorder:
    def __init__(
        self,
        runner: object,
        *,
        task: FrozenTask,
        context: TaskExecutionContext,
        store: LeaseStore,
    ) -> None:
        writer = getattr(runner, "_write_analysis_checkpoint", None)
        if not callable(writer):
            raise DispatchError("direct_candidate_checkpoint_runner_invalid")
        self._writer = writer
        self.task = task
        self.context = context
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not self.store.is_current(self.context.lease):
            raise DispatchError("stale_lease_fence")
        meta = self._writer(*args, **kwargs)
        if not isinstance(meta, Mapping):
            raise DispatchError("direct_candidate_checkpoint_invalid")
        refs = {
            "checkpoint_sha256": meta.get("checkpoint_sha256"),
            "checkpoint_ref": meta.get("checkpoint_ref"),
            "checkpoint_binding_key": meta.get("binding_key"),
            "checkpoint_binding_sha256": meta.get("binding_sha256"),
        }
        hashes = (
            refs["checkpoint_sha256"],
            refs["checkpoint_binding_key"],
            refs["checkpoint_binding_sha256"],
        )
        if (
            any(
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)
                for value in hashes
            )
            or refs["checkpoint_ref"]
            != "study-intake-analysis-checkpoint://sha256/"
            + str(refs["checkpoint_sha256"])
        ):
            raise DispatchError("direct_candidate_checkpoint_invalid")
        self.store.record_task_event(
            self.task,
            self.context.lease,
            "analysis_completed",
            artifact_refs=refs,
        )
        return meta


class CoreCandidateRunner:
    """Execute and publish one exact Candidate in an isolated core Worker."""

    def __init__(
        self,
        config: Mapping[str, Any],
        candidate: Candidate,
        reason: str,
        lease_store: LeaseStore,
        *,
        worker_factory: Callable[..., Worker] = Worker,
        expected_batch_authority: Mapping[str, Any] | None = None,
    ) -> None:
        validate_fixed_model_contract(config)
        self.config = copy.deepcopy(dict(config))
        if expected_batch_authority is not None:
            self.config["_expected_batch_authority"] = copy.deepcopy(
                dict(expected_batch_authority)
            )
        self.candidate = candidate
        self.reason = reason
        self.lease_store = lease_store
        self.worker_factory = worker_factory
        self._record_direct_stage_events = (
            isinstance(candidate.private_context, Mapping)
            and isinstance(
                candidate.private_context.get("processing_host_capture_args"),
                Mapping,
            )
        )
        # Direct-MCP live candidates execute both semantic stages inside the
        # core Worker's single publication call.  Ordinary in-process runners
        # keep the dispatcher's normal two-stage/checkpoint machinery.
        self.executes_full_two_pass_in_analysis = self._record_direct_stage_events
        self._analysis_worker: Worker | None = None
        self._model_result: ModelResult | None = None
        self._published: Mapping[str, Any] | None = None
        self.terminal_outcome: str | None = None
        self.terminal_error_code: str | None = None
        self.terminal_quality_error_code: str | None = None
        self.terminal_report_disposition: str | None = None
        self.terminal_review_stage_count = 0

    def run_analysis(
        self, task: FrozenTask, context: TaskExecutionContext
    ) -> StageResult:
        payload = task.frozen_payload
        if any(
            payload.get(name) != getattr(self.candidate, name)
            for name in ("subject", "capture_id", "study_date", "input_fingerprint")
        ):
            raise DispatchError("candidate_task_binding_mismatch")
        worker = self.worker_factory(self.config)
        contract = payload.get("dispatch_contract")
        if (
            not isinstance(contract, Mapping)
            or contract.get("release_id") != getattr(worker, "release_id", None)
        ):
            raise DispatchError("candidate_release_binding_mismatch")
        validate_dispatch_rule_binding(
            contract,
            subject=self.candidate.subject,
            require_processing_contract=True,
        )
        if contract.get("subject_processing_contract_sha256") != (
            self.candidate.input_binding.get("processing_contract_sha256")
        ):
            raise DispatchError(
                "candidate_processing_contract_binding_mismatch"
            )
        self._analysis_worker = worker
        if self._record_direct_stage_events:
            worker.runner._execute_prompt = _DirectStageEventRecorder(
                worker.runner,
                task=task,
                context=context,
                store=self.lease_store,
            )
            worker.runner._write_analysis_checkpoint = (
                _DirectCheckpointEventRecorder(
                    worker.runner,
                    task=task,
                    context=context,
                    store=self.lease_store,
                )
            )
        capturing = _CapturingRunner(worker.runner)
        worker.runner = capturing
        if not self.lease_store.is_current(context.lease):
            raise DispatchError("stale_lease_fence")
        published = worker.process_claimed_candidate(
            self.candidate,
            (
                "infrastructure_resume"
                if context.resume_from_analysis_checkpoint
                else self.reason
            ),
            write_dashboard=False,
        )
        # The worker owns a nested two-pass pipeline for direct MCP routes.
        # Re-check both cancellation and the lease fence before accepting any
        # nested result or allowing the outer dispatcher to publish it.
        if context.cancel_event.is_set():
            raise DispatchCancelled()
        if not self.lease_store.is_current(context.lease):
            raise DispatchError("stale_lease_fence")
        self._published = copy.deepcopy(dict(published))
        expected_statuses = {
            "math": {
                "ready", "shadow_two_pass_ready", "two_pass_ready",
                "succeeded",
            },
            "cs408": {"two_pass_ready", "succeeded"},
            "english": {"ready", "succeeded"},
        }
        technical_review_failure = bool(
            published.get("status") == "failed"
            and published.get("report_disposition") == "quarantined"
            and isinstance(published.get("review_candidate_stage"), Mapping)
        )
        if (
            published.get("status")
            not in expected_statuses[self.candidate.subject]
            and not technical_review_failure
        ):
            error = published.get("last_error_code") or published.get("error_code")
            raise DispatchError(str(error or "core_publication_failed"))
        results = [row for row in capturing.results if isinstance(row, ModelResult)]
        if not results and published.get("status") in {"succeeded", "failed"}:
            raw_stage = published.get("review_candidate_stage")
            if not isinstance(raw_stage, Mapping):
                raise DispatchError("core_review_candidate_stage_missing")
            stage = StageResult.coerce(raw_stage)
            disposition = str(published.get("report_disposition") or "")
            review_result = raw_stage.get("review_result")
            review_error_code = (
                review_result.get("error_code")
                if isinstance(review_result, Mapping)
                else None
            )
            quarantined = disposition == "quarantined"
            technical_error = (
                published.get("last_error_code")
                or published.get("error_code")
                or review_error_code
                or "review_candidate_identity_invalid"
                if quarantined
                else None
            )
            decision = decide_execution_quality(
                model_completed=stage.model_call_count >= 1,
                provider_completed=stage.provider_request_count >= 2,
                mcp_database_query_completed=stage.mcp_tool_call_count >= 1,
                raw_output_reopenable=bool(
                    stage.raw_output_object_sha256
                    and stage.raw_output_object_ref
                ),
                report_reopenable=raw_stage.get("report_available") is True,
                identity_verified=(
                    stage.runtime_identity_status == "confirmed"
                    and not quarantined
                ),
                quality_findings_present=not quarantined,
                technical_error_code=(
                    str(technical_error) if quarantined else None
                ),
                quarantined=quarantined,
            )
            if (
                published.get("status") != decision.job_status
                or disposition != decision.report_disposition
                or raw_stage.get("execution_status")
                != decision.execution_status
                or raw_stage.get("quality_status") != decision.quality_status
                or raw_stage.get("report_available") is not True
                or raw_stage.get("sol_review_status")
                != decision.sol_review_status
                or raw_stage.get("formal_write_eligible") is not False
                or raw_stage.get("production_accepted") is not False
                or (
                    not quarantined
                    and (
                        not isinstance(review_error_code, str)
                        or not review_error_code
                    )
                )
            ):
                raise DispatchError("core_review_candidate_binding_invalid")
            self.terminal_outcome = decision.terminal_outcome
            self.terminal_error_code = decision.error_code
            self.terminal_quality_error_code = (
                str(review_error_code) if not quarantined else None
            )
            self.terminal_report_disposition = disposition
            self.terminal_review_stage_count = 1
            return stage
        if not results:
            raise DispatchError("core_model_result_invalid")
        two_pass = [row for row in results if row.pipeline_status == "two_pass_ready"]
        quality_review = [
            row
            for row in results
            if row.pipeline_status == "quality_review_ready"
        ]
        result = (
            two_pass[-1]
            if two_pass
            else quality_review[-1]
            if quality_review
            else results[-1]
        )
        self._model_result = result
        if quality_review and not two_pass:
            if (
                self.candidate.subject != "cs408"
                or published.get("status") != "succeeded"
                or not isinstance(result.critical_review, Mapping)
                or result.critical_review.get("verdict") != "reject"
                or published.get("rejection_receipt_sha256") is None
                or published.get("package_id") is not None
                or published.get("package_path") is not None
                or published.get("execution_status") != "succeeded"
                or published.get("quality_status") != "issues_found"
                or published.get("report_disposition") != "needs_sol_review"
                or published.get("sol_review_status") != "pending"
                or published.get("formal_write_eligible") is not False
                or published.get("production_accepted") is not False
            ):
                raise DispatchError("core_quality_review_publication_invalid")
            self.terminal_outcome = "succeeded"
            self.terminal_error_code = None
            self.terminal_quality_error_code = (
                "cs408_critical_review_rejected"
            )
            self.terminal_report_disposition = "needs_sol_review"
            self.terminal_review_stage_count = 2
        elif not two_pass:
            raise DispatchError("core_two_pass_not_ready")
        return _stage_result_from_model(
            result,
            result.draft_analysis
            if isinstance(result.draft_analysis, Mapping)
            else result.analysis,
            "analysis",
            expected_subject=self.candidate.subject,
            expected_release_id=str(contract["release_id"]),
        )

    def run_critical_review(
        self,
        task: FrozenTask,
        draft_analysis: Mapping[str, Any],
        context: TaskExecutionContext,
    ) -> StageResult:
        del draft_analysis
        result = self._model_result
        published = self._published
        if result is None or published is None:
            raise DispatchError("core_analysis_result_missing")
        if not self.lease_store.is_current(context.lease):
            raise DispatchError("stale_lease_fence")
        if not isinstance(result.critical_review, Mapping):
            raise DispatchError("core_critical_review_stage_missing")
        critical_payload = result.critical_review
        return _stage_result_from_model(
            result,
            critical_payload,
            "critical_review",
            expected_subject=self.candidate.subject,
            expected_release_id=str(
                task.frozen_payload["dispatch_contract"]["release_id"]
            ),
        )

    def cancel(self, _context: TaskExecutionContext) -> None:
        worker = self._analysis_worker
        if worker is None:
            return
        cancel = getattr(worker.runner, "cancel_active", None)
        if callable(cancel):
            cancel()


def _stage_identity_from_receipt(
    receipt: Mapping[str, Any], stage_name: str
) -> tuple[str | None, str | None, str, str]:
    """Preserve observed identity without converting a request into evidence."""

    status = receipt.get("runtime_identity_status")
    model = receipt.get("runtime_model")
    effort = receipt.get("runtime_reasoning_effort")
    provenance = str(
        receipt.get("runtime_metadata_provenance") or "unavailable"
    )
    if status == "confirmed":
        if (
            model != REQUIRED_MODEL
            or effort != REQUIRED_REASONING_EFFORT
            or provenance != "codex_json_attestation_v1"
        ):
            raise DispatchError(f"{stage_name}_runtime_identity_invalid")
        return REQUIRED_MODEL, REQUIRED_REASONING_EFFORT, provenance, status
    if status == "requested_unverified":
        if model is not None or effort is not None or provenance != "unavailable":
            raise DispatchError(f"{stage_name}_runtime_identity_invalid")
        return None, None, provenance, status
    raise DispatchError(f"{stage_name}_runtime_identity_invalid")


def _stage_result_from_model(
    result: ModelResult,
    payload: Mapping[str, Any],
    stage_name: str,
    *,
    expected_subject: str,
    expected_release_id: str,
) -> StageResult:
    receipts = result.stage_receipts
    receipt = (
        receipts.get(stage_name)
        if isinstance(receipts, Mapping)
        else None
    )
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("status") != "ready"
        or receipt.get("requested_model") != REQUIRED_MODEL
        or receipt.get("requested_reasoning_effort")
        != REQUIRED_REASONING_EFFORT
    ):
        raise DispatchError(f"{stage_name}_stage_receipt_invalid")
    runtime_model, runtime_effort, provenance, identity_status = (
        _stage_identity_from_receipt(receipt, stage_name)
    )
    processing_binding = receipt.get("processing_binding")
    grounding = receipt.get("mcp_grounding_manifest")
    grounding_items = (
        grounding.get("items") if isinstance(grounding, Mapping) else None
    )
    grounding_core = (
        {key: copy.deepcopy(value) for key, value in grounding.items()
         if key != "manifest_sha256"}
        if isinstance(grounding, Mapping)
        else None
    )
    if (
        not isinstance(processing_binding, Mapping)
        or processing_binding.get("candidate_release_id")
        != expected_release_id
        or not isinstance(grounding, Mapping)
        or not isinstance(grounding_items, list)
        or not grounding_items
        or not isinstance(grounding_core, Mapping)
        or grounding.get("manifest_sha256")
        != receipt.get("mcp_grounding_manifest_sha256")
        or grounding.get("manifest_sha256") != sha256_value(grounding_core)
        or not isinstance(receipt.get("read_session_id"), str)
        or not receipt.get("read_session_id")
        or not isinstance(receipt.get("read_session_manifest_sha256"), str)
        or not isinstance(
            receipt.get("authority_snapshot_manifest_sha256"), str
        )
        or not isinstance(receipt.get("capture_freeze_receipt_sha256"), str)
        or not isinstance(
            receipt.get("mcp_read_session_receipt_sha256"), str
        )
        or not isinstance(receipt.get("mcp_transcript_sha256"), str)
        or not isinstance(receipt.get("evidence_generation"), str)
        or not receipt.get("evidence_generation")
        or not isinstance(
            receipt.get("evidence_authority_fingerprint"), str
        )
    ):
        raise DispatchError(f"{stage_name}_mcp_grounding_closure_invalid")
    consumed_refs: set[str] = set()
    for item in grounding_items:
        if (
            not isinstance(item, Mapping)
            or item.get("subject") != expected_subject
            or item.get("generation") != receipt.get("evidence_generation")
            or not isinstance(item.get("evidence_ref"), str)
            or not item.get("evidence_ref")
        ):
            raise DispatchError(
                f"{stage_name}_mcp_grounding_closure_invalid"
            )
        consumed_refs.add(str(item["evidence_ref"]))

    cited_refs: set[str] = set()
    pending: list[Any] = [payload]
    while pending:
        current = pending.pop()
        if isinstance(current, Mapping):
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
        elif (
            isinstance(current, str)
            and current.startswith(f"mcp-item:{expected_subject}:")
        ):
            cited_refs.add(current)
    # The core validators have already checked the complete allowed-ref set,
    # including analysis refs legitimately reused by the critic.  This bridge
    # additionally requires every stage to freshly cite at least one item it
    # consumed itself, so a critic cannot pass using analysis-only evidence.
    stage_grounded_refs = cited_refs.intersection(consumed_refs)
    if not cited_refs or not stage_grounded_refs:
        raise DispatchError(f"{stage_name}_mcp_grounding_closure_invalid")
    return StageResult(
        payload=copy.deepcopy(dict(payload)),
        runtime_model=runtime_model,
        runtime_reasoning_effort=runtime_effort,
        runtime_metadata_provenance=provenance,
        runtime_identity_status=identity_status,
        duration_ms=int(receipt.get("duration_ms") or 0),
        read_session_id=(
            str(receipt["read_session_id"])
            if isinstance(receipt.get("read_session_id"), str)
            else None
        ),
        read_session_manifest_sha256=str(
            receipt["read_session_manifest_sha256"]
        ),
        authority_snapshot_manifest_sha256=str(
            receipt["authority_snapshot_manifest_sha256"]
        ),
        capture_freeze_receipt_sha256=str(
            receipt["capture_freeze_receipt_sha256"]
        ),
        mcp_read_session_receipt_sha256=str(
            receipt["mcp_read_session_receipt_sha256"]
        ),
        evidence_generation=str(receipt["evidence_generation"]),
        evidence_authority_fingerprint=str(
            receipt["evidence_authority_fingerprint"]
        ),
        evidence_subject=expected_subject,
        evidence_release_id=expected_release_id,
        mcp_grounding_manifest_sha256=str(
            receipt["mcp_grounding_manifest_sha256"]
        ),
        mcp_transcript_sha256=str(receipt["mcp_transcript_sha256"]),
        mcp_consumed_evidence_refs=tuple(sorted(consumed_refs)),
        mcp_cited_evidence_refs=tuple(sorted(cited_refs)),
        mcp_stage_grounded_evidence_refs=tuple(
            sorted(stage_grounded_refs)
        ),
        semantic_stage_count=int(receipt.get("semantic_stage_count") or 0),
        provider_request_count=int(receipt.get("provider_request_count") or 0),
        mcp_tool_call_count=int(receipt.get("mcp_tool_call_count") or 0),
        model_call_count=1,
        consumed_terminal_duplicate_read_count=int(
            receipt.get("consumed_terminal_duplicate_read_count") or 0
        ),
        raw_output_object_sha256=receipt.get("raw_output_object_sha256"),
        raw_output_object_ref=receipt.get("raw_output_object_ref"),
        stage_execution_receipt_sha256=receipt.get(
            "stage_execution_receipt_sha256"
        ),
        stage_execution_receipt_ref=receipt.get(
            "stage_execution_receipt_ref"
        ),
        stage_normalization_receipt_sha256=receipt.get(
            "stage_normalization_receipt_sha256"
        ),
        stage_normalization_receipt_ref=receipt.get(
            "stage_normalization_receipt_ref"
        ),
        normalization_status=receipt.get("normalization_status"),
        normalization_warning_count=int(
            receipt.get("normalization_warning_count") or 0
        ),
        normalization_warnings=tuple(
            copy.deepcopy(dict(row))
            for row in receipt.get("normalization_warnings") or []
            if isinstance(row, Mapping)
        ),
    )


__all__ = [
    "CoreCandidateSubprocessRunner",
    "CoreCandidateRunner",
    "EligibleFrozenCandidate",
    "content_processing_identity",
    "scan_eligible_candidates",
    "validate_concurrent_cs408_candidate",
    "validate_fixed_model_contract",
]


class CoreCandidateSubprocessRunner:
    """Run one frozen production Candidate in its own process group."""

    executes_full_two_pass_in_analysis = True

    def __init__(
        self,
        config_path: Path,
        reason: str | None = None,
        *,
        command: list[str] | None = None,
        expected_batch_authority: Mapping[str, Any] | None = None,
        lease_store: LeaseStore | None = None,
    ) -> None:
        self.config_path = config_path.resolve()
        self.reason = reason
        self.command = command or [
            str(
                Path(__file__).resolve().parents[1]
                / "bin"
                / "preprocess_task_runner.py"
            )
        ]
        self.expected_batch_authority = (
            copy.deepcopy(dict(expected_batch_authority))
            if expected_batch_authority is not None
            else None
        )
        self.lease_store = lease_store
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None
        self._process_start_token: str | None = None
        self._process_exit_done = threading.Event()
        self._process_exit_error: BaseException | None = None
        self._critical: StageResult | None = None
        self.terminal_outcome: str | None = None
        self.terminal_error_code: str | None = None
        self.terminal_quality_error_code: str | None = None
        self.terminal_report_disposition: str | None = None
        self.terminal_review_stage_count = 0

    @staticmethod
    def _pid_absent(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        return False

    @staticmethod
    def _pgid_absent(pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        return False

    @classmethod
    def _wait_process_absent(
        cls, pid: int, pgid: int, *, timeout_seconds: float = 1.0
    ) -> tuple[bool, bool]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            pid_absent = cls._pid_absent(pid)
            pgid_absent = cls._pgid_absent(pgid)
            if pid_absent and pgid_absent:
                return True, True
            if time.monotonic() >= deadline:
                return pid_absent, pgid_absent
            time.sleep(0.01)

    def run_analysis(
        self, task: FrozenTask, context: TaskExecutionContext
    ) -> StageResult:
        contract = task.frozen_payload.get("dispatch_contract")
        frozen_reason = (
            contract.get("dispatch_reason")
            if isinstance(contract, Mapping)
            else None
        )
        if not isinstance(frozen_reason, str) or not frozen_reason:
            if self.reason is None:
                raise DispatchError("task_dispatch_reason_missing")
            frozen_reason = self.reason
        elif self.reason is not None and self.reason != frozen_reason:
            raise DispatchError("task_dispatch_reason_mismatch")
        request = {
            "schema_version": "study-intake-production-task-request-v1",
            "task": task.as_dict(),
            "reason": frozen_reason,
            "unit_sha256": task.unit_sha256,
            "lease_fence": context.lease.fence,
            "lease_owner_id": context.lease.owner_id,
            "execution_mode": (
                "critical_resume"
                if context.resume_from_analysis_checkpoint
                else "full_two_pass"
            ),
        }
        env = os.environ.copy()
        env.update(
            {
                "STUDY_PREPROCESS_UNIT_SHA256": task.unit_sha256,
                "STUDY_PREPROCESS_LEASE_FENCE": str(context.lease.fence),
                "STUDY_PREPROCESS_LEASE_OWNER_ID": context.lease.owner_id,
                "STUDY_PREPROCESS_MODEL": REQUIRED_MODEL,
                "STUDY_PREPROCESS_REASONING_EFFORT": REQUIRED_REASONING_EFFORT,
                "STUDY_PREPROCESS_CONTEXT_ROOT": str(context.root.resolve()),
                "STUDY_PREPROCESS_RUNTIME_ROOT": str(
                    context.root.resolve().parents[3]
                ),
                "STUDY_PREPROCESS_CANCEL_INTENT_PATH": str(
                    (context.root / "cancel-intent.json").resolve()
                ),
            }
        )
        for environment_name in (
            "STUDY_PREPROCESS_EXPECTED_BATCH_ID",
            "STUDY_PREPROCESS_EXPECTED_SCAN_SNAPSHOT_SHA256",
            "STUDY_PREPROCESS_EXPECTED_AUTHORITY_GENERATION",
            "STUDY_PREPROCESS_EXPECTED_AUTHORITY_FINGERPRINT",
        ):
            env.pop(environment_name, None)
        if self.expected_batch_authority is not None:
            authority = self.expected_batch_authority
            required = {
                "batch_id": "STUDY_PREPROCESS_EXPECTED_BATCH_ID",
                "scan_snapshot_sha256": (
                    "STUDY_PREPROCESS_EXPECTED_SCAN_SNAPSHOT_SHA256"
                ),
                "generation": "STUDY_PREPROCESS_EXPECTED_AUTHORITY_GENERATION",
                "authority_fingerprint": (
                    "STUDY_PREPROCESS_EXPECTED_AUTHORITY_FINGERPRINT"
                ),
            }
            if any(
                not isinstance(authority.get(name), str)
                or not authority[name]
                for name in required
            ):
                raise DispatchError("subject_batch_authority_binding_invalid")
            env.update(
                {
                    environment_name: str(authority[name])
                    for name, environment_name in required.items()
                }
            )
        argv = [*self.command, "--config", str(self.config_path)]
        launch_nonce = uuid.uuid4().hex
        env["STUDY_PREPROCESS_PROCESS_LAUNCH_NONCE"] = launch_nonce
        launched_at = (
            dt.datetime.now(dt.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        process = subprocess.Popen(
            argv,
            cwd=context.root,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        identity_refs: Mapping[str, Any] | None = None
        stdout = b""
        stderr = b""
        communicated = False
        termination_reason = "launch_failed"
        self._process_exit_done.clear()
        self._process_exit_error = None
        with self._lock:
            self._process = process
        try:
            child_pgid = os.getpgid(process.pid)
            if child_pgid != process.pid:
                raise DispatchError("task_process_group_identity_invalid")
            try:
                process_start_token = kernel_process_start_token(process.pid)
            except ProcessIdentityError as exc:
                raise DispatchError(
                    "task_process_start_token_unavailable"
                ) from exc
            with self._lock:
                if self._process is process:
                    self._process_start_token = process_start_token
            if self.lease_store is not None:
                identity_refs = self.lease_store.publish_task_process_identity(
                    task,
                    context.lease,
                    child_pid=process.pid,
                    child_pgid=child_pgid,
                    process_start_token=process_start_token,
                    launch_nonce=launch_nonce,
                    launched_at=launched_at,
                    argv=argv,
                    executable_path=Path(self.command[0]),
                    start_new_session=True,
                )
                self.lease_store.record_task_event(
                    task,
                    context.lease,
                    "child_process_started",
                    artifact_refs={
                        "process_identity_sha256": identity_refs[
                            "process_identity_sha256"
                        ],
                        "process_identity_path": identity_refs[
                            "process_identity_path"
                        ],
                    },
                )
                request.update(
                    {
                        "supervisor_process_identity_sha256": identity_refs[
                            "process_identity_sha256"
                        ],
                        "supervisor_process_identity_path": identity_refs[
                            "process_identity_path"
                        ],
                        "supervisor_launch_nonce": launch_nonce,
                    }
                )
            if context.cancel_event.is_set():
                raise DispatchError("dispatch_cancelled")
            stdout, stderr = process.communicate(
                json.dumps(request, ensure_ascii=False, sort_keys=True).encode("utf-8")
            )
            communicated = True
            if context.cancel_event.is_set():
                termination_reason = (
                    "timed_out"
                    if str(context.cancel_reason or "").endswith("_timeout")
                    else "cancelled"
                )
            elif process.returncode == 0:
                termination_reason = "completed"
            else:
                termination_reason = "nonzero"
        finally:
            try:
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=1)
                if not communicated:
                    stdout, stderr = process.communicate()
                    communicated = True
                if context.cancel_event.is_set():
                    termination_reason = (
                        "timed_out"
                        if str(context.cancel_reason or "").endswith("_timeout")
                        else "cancelled"
                    )
                elif termination_reason == "launch_failed" and process.returncode != 0:
                    termination_reason = "nonzero"
                if identity_refs is not None and self.lease_store is not None:
                    pid_absent, pgid_absent = self._wait_process_absent(
                        process.pid, process.pid
                    )
                    if not pid_absent or not pgid_absent:
                        raise DispatchError("task_process_reap_unconfirmed")
                    finished_at = (
                        dt.datetime.now(dt.timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                    exit_refs = self.lease_store.publish_task_process_exit(
                        task,
                        context.lease,
                        process_identity_sha256=str(
                            identity_refs["process_identity_sha256"]
                        ),
                        process_identity_path=str(
                            identity_refs["process_identity_path"]
                        ),
                        returncode=int(process.returncode or 0),
                        termination_reason=termination_reason,
                        reaped=True,
                        process_absent=True,
                        pgid_absent=True,
                        stdout_sha256=hashlib.sha256(stdout).hexdigest(),
                        stdout_size=len(stdout),
                        stderr_sha256=hashlib.sha256(stderr).hexdigest(),
                        stderr_size=len(stderr),
                        finished_at=finished_at,
                    )
                    self.lease_store.record_task_event(
                        task,
                        context.lease,
                        "child_process_exited",
                        artifact_refs={
                            "process_identity_sha256": identity_refs[
                                "process_identity_sha256"
                            ],
                            "process_identity_path": identity_refs[
                                "process_identity_path"
                            ],
                            "process_exit_sha256": exit_refs[
                                "task_process_exit_sha256"
                            ],
                            "process_exit_path": exit_refs[
                                "task_process_exit_path"
                            ],
                        },
                    )
            except BaseException as exc:
                self._process_exit_error = exc
                raise
            finally:
                with self._lock:
                    if self._process is process:
                        self._process = None
                        self._process_start_token = None
                self._process_exit_done.set()
        if context.cancel_event.is_set():
            raise DispatchCancelled()
        if process.returncode != 0:
            try:
                failure = json.loads(stderr.decode("utf-8"))
                code = str(failure.get("error_code") or "task_process_failed")
            except (UnicodeError, json.JSONDecodeError, AttributeError):
                code = f"task_process_exit_{process.returncode}"
            raise DispatchError(code)
        if len(stdout) > 64 * 1024 * 1024:
            raise DispatchError("task_process_output_too_large")
        try:
            value = json.loads(stdout.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError("task_process_output_invalid") from exc
        if (
            not isinstance(value, Mapping)
            or value.get("schema_version")
            != "study-intake-production-task-result-v1"
            or value.get("unit_sha256") != task.unit_sha256
            or value.get("lease_fence") != context.lease.fence
        ):
            raise DispatchError("task_process_result_binding_mismatch")
        terminal_outcome = value.get("terminal_outcome")
        if terminal_outcome is not None:
            if terminal_outcome not in {"succeeded", "failed"}:
                raise DispatchError("task_process_terminal_outcome_invalid")
            disposition = value.get("terminal_report_disposition")
            stage_count = value.get("terminal_review_stage_count")
            terminal_error = value.get("terminal_error_code")
            terminal_quality_error = value.get(
                "terminal_quality_error_code"
            )
            if (
                disposition not in {"needs_sol_review", "quarantined"}
                or isinstance(stage_count, bool)
                or not isinstance(stage_count, int)
                or stage_count not in {1, 2}
            ):
                raise DispatchError("task_process_review_terminal_invalid")
            if (
                terminal_outcome == "succeeded"
                and (
                    disposition != "needs_sol_review"
                    or terminal_error is not None
                    or not isinstance(terminal_quality_error, str)
                    or not terminal_quality_error
                )
            ):
                raise DispatchError("task_process_review_terminal_invalid")
            if terminal_outcome == "failed" and (
                disposition != "quarantined"
                or not isinstance(terminal_error, str)
                or not terminal_error
                or terminal_quality_error is not None
            ):
                raise DispatchError("task_process_review_terminal_invalid")
            quarantined = disposition == "quarantined"
            decision = decide_execution_quality(
                model_completed=True,
                provider_completed=True,
                mcp_database_query_completed=True,
                raw_output_reopenable=True,
                report_reopenable=True,
                identity_verified=not quarantined,
                quality_findings_present=not quarantined,
                technical_error_code=(
                    str(terminal_error)
                    if quarantined
                    else None
                ),
                quarantined=quarantined,
            )
            if any(
                value.get(key) != expected
                for key, expected in decision.publication_fields().items()
            ):
                raise DispatchError("task_process_review_terminal_invalid")
            self.terminal_outcome = str(terminal_outcome)
            self.terminal_error_code = decision.error_code
            self.terminal_quality_error_code = (
                str(terminal_quality_error) if not quarantined else None
            )
            self.terminal_report_disposition = str(disposition)
            self.terminal_review_stage_count = int(stage_count)
        analysis = StageResult.coerce(value.get("analysis"))
        raw_critical = value.get("critical_review")
        self._critical = (
            StageResult.coerce(raw_critical)
            if raw_critical is not None
            else None
        )
        if self.terminal_review_stage_count == 2 and self._critical is None:
            raise DispatchError("task_process_review_stage_missing")
        if self.terminal_review_stage_count == 1 and self._critical is not None:
            raise DispatchError("task_process_review_stage_count_invalid")
        return analysis

    def run_critical_review(
        self,
        task: FrozenTask,
        draft_analysis: Mapping[str, Any],
        context: TaskExecutionContext,
    ) -> StageResult:
        del task, draft_analysis, context
        if self._critical is None:
            raise DispatchError("task_process_critical_result_missing")
        return self._critical

    def probe_stage(
        self,
        task: FrozenTask,
        context: TaskExecutionContext,
        *,
        stage: str,
        nonce: str,
    ) -> dict[str, Any]:
        """Probe the exact Provider, never the task-runner PID as progress."""

        del stage
        if (
            not isinstance(nonce, str)
            or len(nonce) != 32
            or any(char not in "0123456789abcdef" for char in nonce)
        ):
            raise DispatchError("task_liveness_probe_nonce_invalid")
        unsupported = {
            "probe_supported": False,
            "probe_nonce": nonce,
            "control_channel_ok": False,
            "provider_data_plane_ok": False,
            "provider_kernel_activity_ok": False,
            "automatic_stall_cancellation_eligible": False,
        }
        with self._lock:
            process = self._process
            expected_start_token = self._process_start_token
        if (
            process is None
            or process.poll() is not None
            or self.lease_store is None
        ):
            return unsupported
        try:
            actual_start_token = kernel_process_start_token(process.pid)
        except ProcessIdentityError:
            actual_start_token = None
        if actual_start_token != expected_start_token:
            return unsupported
        subject = str(task.frozen_payload.get("subject") or "")
        stage_candidates = [
            f"{subject}_analysis",
            f"{subject}_critical_review",
        ]
        live_identities: list[dict[str, Any]] = []
        for provider_stage in stage_candidates:
            try:
                identity = (
                    self.lease_store.verified_live_provider_process_identity(
                        task,
                        context.lease,
                        stage_name=provider_stage,
                    )
                )
            except (DispatchError, OSError):
                continue
            live_identities.append(identity)
        if len(live_identities) != 1:
            return unsupported
        identity = live_identities[0]
        provider_stage = str(identity["stage_name"])
        try:
            latest_pair = self.lease_store.latest_stage_progress(
                task,
                context.lease,
                stage_name=provider_stage,
            )
        except (DispatchError, OSError):
            latest_pair = None
        baseline_progress_receipt_sha256 = (
            latest_pair.get("progress_receipt_sha256")
            if isinstance(latest_pair, Mapping)
            else None
        )
        try:
            previous_probe_receipt_sha256 = None
            previous_index_path = (
                self.lease_store.provider_kernel_probe_latest_root
                / task.unit_sha256
                / f"fence-{context.lease.fence}"
                / f"{provider_stage}.json"
            )
            previous_index = (
                json.loads(previous_index_path.read_text(encoding="utf-8"))
                if previous_index_path.is_file()
                else None
            )
            previous_receipt = None
            if isinstance(previous_index, Mapping):
                previous_probe_receipt_sha256 = str(
                    previous_index.get(
                        "provider_kernel_probe_receipt_sha256"
                    )
                    or ""
                )
                previous_probe_receipt_path = Path(
                    str(
                        previous_index.get(
                            "provider_kernel_probe_receipt_path"
                        )
                        or ""
                    )
                )
                previous_receipt = json.loads(
                    previous_probe_receipt_path.read_text(encoding="utf-8")
                )
                if (
                    hashlib.sha256(
                        previous_probe_receipt_path.read_bytes()
                    ).hexdigest()
                    != previous_probe_receipt_sha256
                    or previous_receipt.get(
                        "provider_process_identity_sha256"
                    )
                    != identity["provider_process_identity_sha256"]
                    or previous_receipt.get("unit_sha256")
                    != task.unit_sha256
                    or previous_receipt.get("owner_id")
                    != context.lease.owner_id
                    or previous_receipt.get("lease_fence")
                    != context.lease.fence
                    or previous_receipt.get("stage_name") != provider_stage
                ):
                    return unsupported
                baseline_kernel_snapshot = copy.deepcopy(
                    dict(previous_receipt["final_kernel_snapshot"])
                )
            else:
                baseline_kernel_snapshot = kernel_process_activity_snapshot(
                    int(identity["provider_pid"]),
                    expected_start_token=str(
                        identity["process_start_token"]
                    ),
                    expected_pgid=int(identity["provider_pgid"]),
                )
            baseline_kernel_snapshot_sha256 = (
                kernel_activity_snapshot_sha256(
                    baseline_kernel_snapshot
                )
            )
        except (ProcessIdentityError, OSError, TypeError, ValueError):
            return unsupported
        probe_started_at = (
            dt.datetime.now(dt.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        sample_started = time.monotonic()
        probe_root = context.root / "stall-probes"
        probe_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        request_path = probe_root / "request.json"
        response_path = probe_root / "response.json"
        try:
            response_path.unlink()
        except FileNotFoundError:
            pass
        request = {
            "schema_version": "study-intake-task-liveness-probe-v1",
            "unit_sha256": task.unit_sha256,
            "lease_fence": context.lease.fence,
            "provider_stage_name": provider_stage,
            "baseline_progress_receipt_sha256": (
                baseline_progress_receipt_sha256
            ),
            "provider_process_identity_sha256": identity[
                "provider_process_identity_sha256"
            ],
            "provider_pid": identity["provider_pid"],
            "provider_pgid": identity["provider_pgid"],
            "process_start_token": identity["process_start_token"],
            "baseline_kernel_snapshot_sha256": (
                baseline_kernel_snapshot_sha256
            ),
            "nonce": nonce,
            "formal_write_count": 0,
        }
        temp_path = request_path.with_name(f".{request_path.name}.{nonce}.tmp")
        temp_path.write_text(
            json.dumps(request, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temp_path, request_path)
        try:
            os.kill(process.pid, signal.SIGUSR1)
        except ProcessLookupError:
            return unsupported
        deadline = time.monotonic() + 5.0
        response: Mapping[str, Any] | None = None
        while time.monotonic() < deadline:
            try:
                raw_response = json.loads(response_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                time.sleep(0.05)
                continue
            if isinstance(raw_response, Mapping):
                response = raw_response
                break
        control_ok = bool(
            response is not None
            and response.get("schema_version")
            == "study-intake-task-liveness-probe-response-v1"
            and response.get("unit_sha256") == task.unit_sha256
            and response.get("lease_fence") == context.lease.fence
            and response.get("nonce") == nonce
            and response.get("control_channel_ok") is True
        )
        response_identity_verified = bool(
            control_ok
            and response.get("provider_process_identity_sha256")
            == identity["provider_process_identity_sha256"]
            and response.get("provider_pid") == identity["provider_pid"]
            and response.get("provider_pgid") == identity["provider_pgid"]
            and response.get("process_start_token")
            == identity["process_start_token"]
            and response.get("baseline_kernel_snapshot_sha256")
            == baseline_kernel_snapshot_sha256
        )
        remaining_sample_window = 0.05 - (
            time.monotonic() - sample_started
        )
        if remaining_sample_window > 0:
            time.sleep(remaining_sample_window)
        try:
            final_kernel_snapshot = kernel_process_activity_snapshot(
                int(identity["provider_pid"]),
                expected_start_token=str(identity["process_start_token"]),
                expected_pgid=int(identity["provider_pgid"]),
            )
            kernel_delta = kernel_process_activity_delta(
                baseline_kernel_snapshot,
                final_kernel_snapshot,
            )
        except (ProcessIdentityError, OSError, TypeError, ValueError):
            return unsupported
        try:
            latest = self.lease_store.latest_stage_progress(
                task, context.lease, stage_name=provider_stage
            )
        except (DispatchError, OSError):
            latest = None
        progress_sha256 = (
            latest.get("progress_receipt_sha256")
            if isinstance(latest, Mapping)
            else None
        )
        data_plane_ok = bool(
            isinstance(latest, Mapping)
            and isinstance(progress_sha256, str)
            and progress_sha256 != baseline_progress_receipt_sha256
            and latest.get("progress_kind")
            in {"provider_output", "mcp_call"}
            and latest.get("provider_pid") == identity["provider_pid"]
            and latest.get("provider_pgid") == identity["provider_pgid"]
            and latest.get("process_start_token")
            == identity["process_start_token"]
        )
        probe_finished_at = (
            dt.datetime.now(dt.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        try:
            receipt_refs = (
                self.lease_store.publish_provider_kernel_probe_receipt(
                    task,
                    context.lease,
                    stage_name=provider_stage,
                    provider_process_identity_sha256=str(
                        identity["provider_process_identity_sha256"]
                    ),
                    provider_process_identity_path=str(
                        identity["provider_process_identity_path"]
                    ),
                    probe_nonce_sha256=hashlib.sha256(
                        nonce.encode("ascii")
                    ).hexdigest(),
                    previous_probe_receipt_sha256=(
                        previous_probe_receipt_sha256
                    ),
                    baseline_progress_receipt_sha256=(
                        str(baseline_progress_receipt_sha256)
                        if baseline_progress_receipt_sha256 is not None
                        else None
                    ),
                    observed_progress_receipt_sha256=(
                        str(progress_sha256)
                        if progress_sha256 is not None
                        else None
                    ),
                    control_channel_ok=control_ok,
                    response_identity_verified=response_identity_verified,
                    signed_progress_delta_ok=data_plane_ok,
                    baseline_kernel_snapshot=baseline_kernel_snapshot,
                    final_kernel_snapshot=final_kernel_snapshot,
                    kernel_activity_delta=kernel_delta,
                    probe_started_at=probe_started_at,
                    probe_finished_at=probe_finished_at,
                )
            )
        except (DispatchError, OSError, ProcessIdentityError):
            return unsupported
        return {
            "probe_supported": True,
            "probe_nonce": nonce,
            "control_channel_ok": control_ok,
            "provider_data_plane_ok": data_plane_ok,
            "provider_kernel_activity_ok": bool(
                kernel_delta["activity_detected"]
            ),
            "automatic_stall_cancellation_eligible": True,
            "provider_data_plane_receipt_sha256": (
                progress_sha256 if data_plane_ok else None
            ),
            "provider_kernel_probe_receipt_sha256": receipt_refs[
                "provider_kernel_probe_receipt_sha256"
            ],
            "provider_kernel_probe_receipt_path": receipt_refs[
                "provider_kernel_probe_receipt_path"
            ],
            "provider_process_identity_sha256": identity[
                "provider_process_identity_sha256"
            ],
            "provider_pid": identity["provider_pid"],
            "provider_pgid": identity["provider_pgid"],
            "process_start_token": identity["process_start_token"],
        }

    def cancel(self, context: TaskExecutionContext) -> None:
        intent_path = context.root / "cancel-intent.json"
        intent = {
            "schema_version": "study-intake-task-cancel-intent-v1",
            "unit_sha256": context.task.unit_sha256,
            "lease_fence": context.lease.fence,
            "reason": str(context.cancel_reason or "cancelled"),
            "formal_write_count": 0,
        }
        temp_path = intent_path.with_name(
            f".{intent_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            temp_path.write_text(
                json.dumps(
                    intent,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            os.replace(temp_path, intent_path)
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
        with self._lock:
            process = self._process
        if process is None:
            return
        # The supervisor may have exited while its Provider grandchild still
        # owns the original process group.  Signal the group even when the
        # supervisor has already been reaped so the exit receipt can close
        # before the dispatcher publishes the cancelled terminal.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        if process.poll() is None:
            try:
                # The one-shot runner first closes and reaps the real Codex
                # child, then publishes its HMAC exit artifact.
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    return
        if not self._process_exit_done.wait(timeout=2):
            raise DispatchError("task_process_exit_receipt_timeout")
        if self._process_exit_error is not None:
            raise DispatchError("task_process_exit_receipt_failed")
