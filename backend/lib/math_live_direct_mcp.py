"""Candidate-bound direct-MCP execution units for live math captures.

This module is deliberately zero-model.  It validates the sole live-business
authority, converts every validated task through the public fixture-to-Host
helper, and builds the exact ``Candidate``/``FrozenTask`` pair consumed by the
normal production Dispatcher.  Semantic evidence and local paths live only in
``Candidate.private_context``; the Luna bootstrap remains capture-ID and
read-session metadata only.
"""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from concurrent_dispatch import (
    DispatchError,
    FrozenTask,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    dispatch_rule_binding,
)
from math_live_business_fixture import (
    REAL_AUTHORITY_SCHEMA,
    MathLiveBusinessFixtureError,
    validate_manifest,
    validated_task_to_processing_host_capture_args,
)
from preprocessor_core import (
    LOADED_CORE_SHA256,
    Candidate,
    math_processing_contract,
    sha256_value,
)


DIRECT_MCP_CONTRACT_SCHEMA = "study-intake-math-live-direct-mcp-execution-v1"
DIRECT_MCP_DISPATCH_REASON = "math_live_direct_mcp"
DIRECT_MCP_NAMESPACE = "kaoyan_math_read"
LIVE_CAPTURE_IDS = (
    "LUNA-MATH-20260809-001",
    "LUNA-MATH-20260809-002",
    "LUNA-MATH-20260809-003",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_RUNTIME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")


class MathLiveDirectMcpError(RuntimeError):
    """Stable fail-closed error code for the live direct-MCP lane."""


@dataclass(frozen=True)
class LiveMathDirectMcpUnit:
    task: FrozenTask
    candidate: Candidate
    business_task_id: str
    task_kind: str
    formal_id: str | None
    source_locator: str
    host_capture_args_sha256: str


@dataclass(frozen=True)
class LiveMathDirectMcpExecution:
    authority_manifest_path: Path
    authority_manifest_sha256: str
    fixture_tree_sha256: str
    release_id: str
    execution_attempt: int
    execution_runtime_id: str
    units: tuple[LiveMathDirectMcpUnit, ...]
    scope_sha256: str

    @property
    def tasks(self) -> tuple[FrozenTask, ...]:
        return tuple(unit.task for unit in self.units)

    @property
    def candidates(self) -> tuple[Candidate, ...]:
        return tuple(unit.candidate for unit in self.units)

    def public_receipt(self) -> dict[str, Any]:
        """Return a path- and body-free deterministic preflight receipt."""

        return {
            "schema_version": DIRECT_MCP_CONTRACT_SCHEMA,
            "status": "ready_for_real_luna_direct_mcp",
            "subject": "math",
            "release_id": self.release_id,
            "authority_manifest_sha256": self.authority_manifest_sha256,
            "fixture_tree_sha256": self.fixture_tree_sha256,
            "execution_attempt": self.execution_attempt,
            "execution_runtime_id": self.execution_runtime_id,
            "scope_sha256": self.scope_sha256,
            "mcp_namespace": DIRECT_MCP_NAMESPACE,
            "stage_order": ["analysis", "critical_review"],
            "fresh_critical_review_context": True,
            "expected_model_call_count_per_task": 2,
            "task_count": len(self.units),
            "tasks": [
                {
                    "capture_id": unit.candidate.capture_id,
                    "business_task_id": unit.business_task_id,
                    "task_kind": unit.task_kind,
                    "formal_id": unit.formal_id,
                    "source_locator": unit.source_locator,
                    "input_fingerprint": unit.candidate.input_fingerprint,
                    "unit_sha256": unit.task.unit_sha256,
                    "frozen_payload_sha256": unit.task.frozen_payload_sha256,
                    "host_capture_args_sha256": unit.host_capture_args_sha256,
                    "proposal_only": True,
                    "formal_write_count": 0,
                }
                for unit in self.units
            ],
            "model_call_count": 0,
            "formal_write_count": 0,
        }


def _fail(code: str) -> None:
    raise MathLiveDirectMcpError(code)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _artifact_metadata(
    capture_args: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build model-invisible package metadata without reading evidence bodies."""

    raw = capture_args.get("capture_artifacts")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        _fail("math_live_direct_mcp_capture_artifacts_invalid")
    images: list[dict[str, Any]] = []
    texts: list[dict[str, Any]] = []
    index: list[dict[str, Any]] = []
    for descriptor in raw:
        if not isinstance(descriptor, Mapping):
            _fail("math_live_direct_mcp_capture_artifacts_invalid")
        artifact_id = descriptor.get("artifact_id")
        artifact_kind = descriptor.get("artifact_kind")
        path_value = descriptor.get("path")
        digest = descriptor.get("sha256")
        if (
            not isinstance(artifact_id, str)
            or not artifact_id
            or not isinstance(artifact_kind, str)
            or not isinstance(path_value, str)
            or not Path(path_value).is_absolute()
            or not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
        ):
            _fail("math_live_direct_mcp_capture_artifacts_invalid")
        path = Path(path_value)
        try:
            node = path.lstat()
        except OSError as exc:
            raise MathLiveDirectMcpError(
                "math_live_direct_mcp_capture_artifact_missing"
            ) from exc
        if path.is_symlink() or not path.is_file() or node.st_size <= 0:
            _fail("math_live_direct_mcp_capture_artifacts_invalid")
        if _sha256_file(path) != digest:
            _fail("math_live_direct_mcp_capture_artifact_drift")
        row = {
            "artifact_id": artifact_id,
            "artifact_kind": artifact_kind,
            "sha256": digest,
            "size": node.st_size,
            "content_route": "mcp_read_task_artifact",
        }
        index.append(copy.deepcopy(row))
        if artifact_kind in {"question_image", "solution_image"}:
            images.append(
                {
                    "role": artifact_kind,
                    "media_type": (
                        "image/png"
                        if path.suffix.lower() == ".png"
                        else "application/octet-stream"
                    ),
                    "sha256": digest,
                    "size": node.st_size,
                    "evidence_ref": None,
                    "provided_to_model": False,
                    "source_kind": "live_business_capture_mcp",
                }
            )
        else:
            texts.append(
                {
                    "role": artifact_kind,
                    "sha256": digest,
                    "size": node.st_size,
                    "evidence_ref": None,
                    "source_kind": "live_business_capture_mcp",
                }
            )
    ids = [str(row["artifact_id"]) for row in index]
    if len(ids) != len(set(ids)) or not ids:
        _fail("math_live_direct_mcp_capture_artifacts_invalid")
    return images, texts, sorted(index, key=lambda row: str(row["artifact_id"]))


def _candidate_for_task(
    *,
    config: Mapping[str, Any],
    release_id: str,
    authority: Mapping[str, Any],
    task: Mapping[str, Any],
    execution_attempt: int,
    execution_runtime_id: str,
) -> LiveMathDirectMcpUnit:
    try:
        capture_args = validated_task_to_processing_host_capture_args(task)
    except MathLiveBusinessFixtureError as exc:
        raise MathLiveDirectMcpError(
            "math_live_direct_mcp_capture_args_invalid"
        ) from exc
    images, text_sources, artifact_index = _artifact_metadata(capture_args)
    contract = math_processing_contract(config)
    if not isinstance(contract, Mapping):
        _fail("math_live_direct_mcp_processing_contract_missing")
    processing_sha = contract.get("processing_contract_sha256")
    if not isinstance(processing_sha, str) or SHA256_RE.fullmatch(processing_sha) is None:
        _fail("math_live_direct_mcp_processing_contract_invalid")

    capture_id = str(task["capture_id"])
    formal_id = task.get("formal_id")
    source_locator = str(task["source_locator"])
    source_route = "old_existing" if isinstance(formal_id, str) else "new_intake"
    target_group_key = (
        f"formal:{formal_id}" if isinstance(formal_id, str) else f"source:{source_locator}"
    )
    target_identity = {
        "formal_card_id": formal_id,
        "delivered_card_id": formal_id,
        "knowledge_fallback_card_ids": [],
        "target_group_key": target_group_key,
        "role_contract": {
            "formal_card_id": "existing formal target or null for a new source",
            "delivered_card_id": "actual reviewed formal target or null",
            "knowledge_fallback_card_ids": "retrieval anchors only, never identity",
        },
    }
    host_args_sha256 = sha256_value(capture_args)
    authority_sha = str(authority["authority_manifest_sha256"])
    fixture_tree_sha = str(authority["fixture_tree_sha256"])
    input_binding = {
        "candidate_kind": "math_live_business_task",
        "processing_contract_sha256": processing_sha,
        "loaded_core_sha256": LOADED_CORE_SHA256,
        "capture_recorded_at": task["captured_at"],
        "target_identity": target_identity,
        "source_route": source_route,
        "evidence_manifest_sha256": task["manifest_sha256"],
        "evidence_bundle_sha256": task["content_fingerprint"],
        "image_evidence_refs": [],
        "host_semantic_prefetch": False,
        "processing_host_capture_args_sha256": host_args_sha256,
        "math_live_authority_schema_version": REAL_AUTHORITY_SCHEMA,
        "math_live_business_manifest_sha256": authority_sha,
        "math_live_fixture_tree_sha256": fixture_tree_sha,
        "math_live_content_fingerprint": task["content_fingerprint"],
        "math_live_business_task_id": task["business_task_id"],
        "math_live_task_kind": task["task_kind"],
        "math_live_source_locator": source_locator,
        "math_live_formal_id": formal_id,
        "direct_mcp_namespace": DIRECT_MCP_NAMESPACE,
        "execution_attempt": execution_attempt,
        "execution_runtime_id": execution_runtime_id,
        "proposal_only": True,
        "sol_authorized": False,
        "formal_write_count": 0,
    }
    model_input = {
        "schema_version": "math-live-direct-mcp-candidate-input-v1",
        "candidate_kind": "math_live_business_task",
        "target_identity": target_identity,
        "source_bundle": {
            "schema_version": "math-live-direct-mcp-source-bundle-v1",
            "source_kind": source_route,
            "artifacts": images,
            "text_sources": text_sources,
            "content_route": "kaoyan_math_read.read_task_artifact",
            "host_semantic_prefetch": False,
        },
        "current_question_evidence": artifact_index,
        "image_evidence_refs": [],
        "host_semantic_prefetch": False,
        "proposal_only": True,
        "formal_write_count": 0,
    }
    # The only paths are held out-of-band for the Host freeze.  Neither
    # model_input nor input_binding contains semantic bodies or local paths.
    candidate = Candidate(
        subject="math",
        capture_id=capture_id,
        study_date=str(task["study_date"]),
        recorded_at=str(task["captured_at"]),
        input_fingerprint=str(task["content_fingerprint"]),
        input_binding=input_binding,
        model_input=model_input,
        allowed_evidence_refs=(),
        image_paths=(),
        target_label=target_group_key,
        canonical_state="awaiting_background_analysis",
        sol_state="pending_review",
        private_context={
            "processing_host_capture_args": copy.deepcopy(capture_args),
        },
    )
    base_payload = dict(FrozenTask.from_candidate(candidate).frozen_payload)
    base_payload["dispatch_contract"] = {
        "schema_version": "study-intake-dispatch-release-binding-v1",
        **dispatch_rule_binding(
            release_id=release_id,
            subject="math",
            subject_processing_contract_sha256=processing_sha,
        ),
        "loaded_core_sha256": LOADED_CORE_SHA256,
        "dispatch_reason": DIRECT_MCP_DISPATCH_REASON,
        "model_enqueue_allowed": True,
        "execution_attempt": execution_attempt,
        "execution_runtime_id": execution_runtime_id,
        "math_live_business_manifest_sha256": authority_sha,
        "math_live_content_fingerprint": task["content_fingerprint"],
        "proposal_only": True,
        "formal_write_count": 0,
    }
    frozen = FrozenTask(base_payload)
    return LiveMathDirectMcpUnit(
        task=frozen,
        candidate=candidate,
        business_task_id=str(task["business_task_id"]),
        task_kind=str(task["task_kind"]),
        formal_id=str(formal_id) if isinstance(formal_id, str) else None,
        source_locator=source_locator,
        host_capture_args_sha256=host_args_sha256,
    )


def build_live_math_direct_mcp_execution(
    *,
    config: Mapping[str, Any],
    release_id: str,
    authority_manifest_path: Path,
    execution_attempt: int,
    execution_runtime_id: str,
) -> LiveMathDirectMcpExecution:
    """Build all three executable live-math units without invoking a model."""

    model = config.get("model")
    if (
        not isinstance(model, Mapping)
        or model.get("model") != REQUIRED_MODEL
        or model.get("reasoning_effort") != REQUIRED_REASONING_EFFORT
    ):
        _fail("math_live_direct_mcp_model_contract_invalid")
    if not isinstance(release_id, str) or SHA256_RE.fullmatch(release_id) is None:
        _fail("math_live_direct_mcp_release_invalid")
    if (
        isinstance(execution_attempt, bool)
        or execution_attempt not in {1, 2, 3}
        or not isinstance(execution_runtime_id, str)
        or SAFE_RUNTIME_RE.fullmatch(execution_runtime_id) is None
    ):
        _fail("math_live_direct_mcp_execution_identity_invalid")
    try:
        authority = validate_manifest(authority_manifest_path)
    except MathLiveBusinessFixtureError as exc:
        raise MathLiveDirectMcpError(
            "math_live_direct_mcp_authority_invalid"
        ) from exc
    tasks = authority.get("tasks")
    if (
        authority.get("authority_schema_version") != REAL_AUTHORITY_SCHEMA
        or authority.get("status") != "passed_real_luna_business_preflight"
        or authority.get("task_count") != 3
        or authority.get("model_call_count") != 0
        or authority.get("formal_write_count") != 0
        or authority.get("model_request")
        != {
            "model": REQUIRED_MODEL,
            "reasoning_effort": REQUIRED_REASONING_EFFORT,
            "runtime_attestation": "requested_unverified",
        }
        or not isinstance(tasks, list)
        or tuple(str(row.get("capture_id")) for row in tasks) != LIVE_CAPTURE_IDS
    ):
        _fail("math_live_direct_mcp_authority_contract_invalid")
    units = tuple(
        _candidate_for_task(
            config=config,
            release_id=release_id,
            authority=authority,
            task=task,
            execution_attempt=execution_attempt,
            execution_runtime_id=execution_runtime_id,
        )
        for task in tasks
    )
    if len({unit.task.unit_sha256 for unit in units}) != 3:
        _fail("math_live_direct_mcp_unit_identity_collision")
    scope = {
        "schema_version": DIRECT_MCP_CONTRACT_SCHEMA,
        "release_id": release_id,
        "authority_manifest_sha256": authority["authority_manifest_sha256"],
        "fixture_tree_sha256": authority["fixture_tree_sha256"],
        "execution_attempt": execution_attempt,
        "execution_runtime_id": execution_runtime_id,
        "mcp_namespace": DIRECT_MCP_NAMESPACE,
        "stage_order": ["analysis", "critical_review"],
        "fresh_critical_review_context": True,
        "expected_model_call_count_per_task": 2,
        "units": [
            {
                "capture_id": unit.candidate.capture_id,
                "input_fingerprint": unit.candidate.input_fingerprint,
                "unit_sha256": unit.task.unit_sha256,
                "host_capture_args_sha256": unit.host_capture_args_sha256,
            }
            for unit in units
        ],
        "proposal_only": True,
        "formal_write_count": 0,
    }
    return LiveMathDirectMcpExecution(
        authority_manifest_path=authority_manifest_path.expanduser().resolve(),
        authority_manifest_sha256=str(authority["authority_manifest_sha256"]),
        fixture_tree_sha256=str(authority["fixture_tree_sha256"]),
        release_id=release_id,
        execution_attempt=execution_attempt,
        execution_runtime_id=execution_runtime_id,
        units=units,
        scope_sha256=sha256_value(scope),
    )


def replace_candidate_bound_live_math_tasks(
    tasks: Sequence[FrozenTask],
    execution: LiveMathDirectMcpExecution,
) -> tuple[tuple[FrozenTask, ...], str]:
    """Replace static live placeholders while retaining the three goldens.

    Candidate-bound specs prove which three business captures are authorized.
    Each staircase attempt still needs a distinct unit and private in-memory
    Candidate, so the static placeholders are checked and then replaced by the
    attempt-bound executable units.
    """

    live_ids = set(LIVE_CAPTURE_IDS)
    placeholders: dict[str, FrozenTask] = {}
    regular: list[FrozenTask] = []
    for task in tasks:
        capture_id = str(task.frozen_payload.get("capture_id") or "")
        if capture_id in live_ids:
            if capture_id in placeholders:
                _fail("math_live_direct_mcp_placeholder_duplicate")
            placeholders[capture_id] = task
        else:
            regular.append(task)
    if set(placeholders) != live_ids or len(regular) != 3:
        _fail("math_live_direct_mcp_placeholder_set_invalid")
    for unit in execution.units:
        placeholder = placeholders[unit.candidate.capture_id]
        binding = placeholder.frozen_payload.get("input_binding")
        if (
            placeholder.frozen_payload.get("input_fingerprint")
            != unit.candidate.input_fingerprint
            or not isinstance(binding, Mapping)
            or binding.get("math_live_business_manifest_sha256")
            != execution.authority_manifest_sha256
            or binding.get("math_live_content_fingerprint")
            != unit.candidate.input_fingerprint
        ):
            _fail("math_live_direct_mcp_placeholder_binding_invalid")
    combined = (*execution.tasks, *regular)
    scope_sha256 = sha256_value(
        {
            "schema_version": "mixed-math-live-direct-mcp-scope-v1",
            "live_execution_scope_sha256": execution.scope_sha256,
            "ordered_unit_sha256s": [task.unit_sha256 for task in combined],
            "formal_write_count": 0,
        }
    )
    return tuple(combined), scope_sha256


def register_live_math_direct_candidates(
    runtime: Any,
    execution: LiveMathDirectMcpExecution,
) -> None:
    """Register Host-private Candidates before the zero-model batch freeze."""

    register = getattr(runtime, "register_controlled_replay_candidate", None)
    if not callable(register):
        _fail("math_live_direct_mcp_runtime_registration_missing")
    for unit in execution.units:
        try:
            register(
                unit.task,
                unit.candidate,
                reason=DIRECT_MCP_DISPATCH_REASON,
            )
        except DispatchError as exc:
            raise MathLiveDirectMcpError(exc.code) from exc


__all__ = [
    "DIRECT_MCP_CONTRACT_SCHEMA",
    "DIRECT_MCP_DISPATCH_REASON",
    "DIRECT_MCP_NAMESPACE",
    "LIVE_CAPTURE_IDS",
    "LiveMathDirectMcpExecution",
    "LiveMathDirectMcpUnit",
    "MathLiveDirectMcpError",
    "build_live_math_direct_mcp_execution",
    "register_live_math_direct_candidates",
    "replace_candidate_bound_live_math_tasks",
]
