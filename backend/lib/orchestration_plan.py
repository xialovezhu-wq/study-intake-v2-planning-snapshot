"""Strict read-plan validation and dependency-layer construction."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping


SUBJECTS = frozenset({"math", "cs408", "english"})
READ_TOOLS = frozenset(
    {"get_task_context", "read_task_artifact", "list_records", "get_records", "search_records", "query_relations"}
)
FORBIDDEN_TOKENS = ("write", "apply", "shell", "terminal", "web", "sql", "delete", "update")


class ReadPlanError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)


def _branch_signature(branch: Mapping[str, Any]) -> str:
    semantic = {key: copy.deepcopy(value) for key, value in branch.items() if key not in {"branch_id", "rationale"}}
    return sha256_value(semantic)


def validate_read_plan(value: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema_version", "plan_id", "subject", "capture_id", "frozen_task_sha256",
        "release_id", "activation_id", "authority_snapshot_sha256", "generation",
        "orchestrate_skill", "terra_agent_contract_sha256", "created_at", "branches",
        "formal_write_count", "plan_sha256",
    }
    if set(value) != required or value.get("schema_version") != "orchestration_read_plan_v1":
        raise ReadPlanError("read_plan_shape_invalid")
    if value.get("subject") not in SUBJECTS or value.get("formal_write_count") != 0:
        raise ReadPlanError("read_plan_identity_invalid")
    for key in ("frozen_task_sha256", "release_id", "activation_id", "authority_snapshot_sha256", "terra_agent_contract_sha256"):
        if not _sha(value.get(key)):
            raise ReadPlanError("read_plan_hash_invalid")
    skill = value.get("orchestrate_skill")
    if (
        not isinstance(skill, Mapping)
        or set(skill) != {"id", "version", "sha256"}
        or not _sha(skill.get("sha256"))
        or not isinstance(skill.get("id"), str)
        or not isinstance(skill.get("version"), str)
    ):
        raise ReadPlanError("read_plan_skill_binding_invalid")
    branches = value.get("branches")
    if not isinstance(branches, list) or not branches:
        raise ReadPlanError("read_plan_empty")
    branch_ids: set[str] = set()
    signatures: set[str] = set()
    normalized: list[dict[str, Any]] = []
    branch_keys = {
        "branch_id", "purpose", "rationale", "required", "depends_on",
        "allowed_task_artifact_ids", "allowed_mcp_tools", "collection_scope",
        "query_constraints", "maximum_calls", "maximum_records", "maximum_bytes",
        "completion_requirements", "failure_policy", "expected_evidence_kinds",
    }
    for raw in branches:
        if not isinstance(raw, Mapping) or set(raw) != branch_keys:
            raise ReadPlanError("read_branch_shape_invalid")
        branch = copy.deepcopy(dict(raw))
        branch_id = branch.get("branch_id")
        if not isinstance(branch_id, str) or not branch_id or branch_id in branch_ids:
            raise ReadPlanError("read_branch_id_invalid")
        branch_ids.add(branch_id)
        if not isinstance(branch.get("required"), bool):
            raise ReadPlanError("read_branch_required_invalid")
        for key in ("depends_on", "allowed_task_artifact_ids", "allowed_mcp_tools", "collection_scope", "completion_requirements", "expected_evidence_kinds"):
            rows = branch.get(key)
            if not isinstance(rows, list) or (key in {"allowed_task_artifact_ids", "allowed_mcp_tools", "completion_requirements"} and not rows):
                raise ReadPlanError("read_branch_collection_invalid")
            if any(not isinstance(item, str) or not item for item in rows) or len(rows) != len(set(rows)):
                raise ReadPlanError("read_branch_collection_invalid")
        tools = set(branch["allowed_mcp_tools"])
        if not tools <= READ_TOOLS or any(any(token in tool.lower() for token in FORBIDDEN_TOKENS) for tool in tools):
            raise ReadPlanError("read_branch_forbidden_tool")
        for key in ("maximum_calls", "maximum_records", "maximum_bytes"):
            count = branch.get(key)
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
                raise ReadPlanError("read_branch_budget_invalid")
        if branch.get("failure_policy") not in {"fail_closed", "report_issue", "optional_missing"}:
            raise ReadPlanError("read_branch_failure_policy_invalid")
        signature = _branch_signature(branch)
        if signature in signatures:
            raise ReadPlanError("read_branch_duplicate_semantics")
        signatures.add(signature)
        normalized.append(branch)
    for branch in normalized:
        dependencies = branch["depends_on"]
        if branch["branch_id"] in dependencies or any(dep not in branch_ids for dep in dependencies):
            raise ReadPlanError("read_branch_dependency_invalid")
    layers = dependency_layers({branch["branch_id"]: branch["depends_on"] for branch in normalized})
    core = {key: copy.deepcopy(value[key]) for key in value if key != "plan_sha256"}
    if value.get("plan_sha256") != sha256_value(core):
        raise ReadPlanError("read_plan_digest_invalid")
    result = copy.deepcopy(dict(value))
    result["dependency_layers"] = layers
    return result


def dependency_layers(graph: Mapping[str, list[str]]) -> list[list[str]]:
    remaining = {node: set(dependencies) for node, dependencies in graph.items()}
    layers: list[list[str]] = []
    completed: set[str] = set()
    while remaining:
        ready = sorted(node for node, dependencies in remaining.items() if dependencies <= completed)
        if not ready:
            raise ReadPlanError("read_plan_dependency_cycle")
        layers.append(ready)
        completed.update(ready)
        for node in ready:
            del remaining[node]
    return layers


def seal_read_plan(core: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(core))
    if "plan_sha256" in value:
        raise ReadPlanError("read_plan_already_sealed")
    value["plan_sha256"] = sha256_value(value)
    validate_read_plan(value)
    return value
