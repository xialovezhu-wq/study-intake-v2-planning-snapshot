"""Role-separated Terra/Luna model contract for Multi-Agent V2."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


ROLE_SPECS = {
    "orchestrator": {
        "model": "gpt-5.6-terra",
        "reasoning_effort": "ultra",
        "agents_enabled": True,
        "fresh_context": True,
        "sandbox_mode": "workspace-write",
    },
    "reader": {
        "model": "gpt-5.6-luna",
        "reasoning_effort": "max",
        "agents_enabled": False,
        "fresh_context": True,
        "sandbox_mode": "read-only",
    },
    "critical_reviewer": {
        "model": "gpt-5.6-terra",
        "reasoning_effort": "ultra",
        "agents_enabled": False,
        "fresh_context": True,
        "sandbox_mode": "read-only",
    },
}


class ModelRoleContractError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def model_contract_from_config(config: Mapping[str, Any]) -> dict[str, Any]:
    models = config.get("models")
    if not isinstance(models, Mapping) or set(models) != set(ROLE_SPECS):
        raise ModelRoleContractError("multi_agent_model_roles_invalid")
    normalized: dict[str, dict[str, Any]] = {}
    for role, required in ROLE_SPECS.items():
        supplied = models.get(role)
        if not isinstance(supplied, Mapping):
            raise ModelRoleContractError("multi_agent_model_role_invalid")
        expected_keys = set(required) | {"agent_config_path", "tool_policy_path"}
        if set(supplied) != expected_keys or any(supplied.get(key) != value for key, value in required.items()):
            raise ModelRoleContractError(f"multi_agent_{role}_contract_invalid")
        assets: dict[str, str] = {}
        for field in ("agent_config_path", "tool_policy_path"):
            raw = supplied.get(field)
            if not isinstance(raw, str) or not Path(raw).is_absolute():
                raise ModelRoleContractError(f"multi_agent_{role}_asset_path_invalid")
            path = Path(raw)
            if path.is_symlink() or not path.is_file():
                raise ModelRoleContractError(f"multi_agent_{role}_asset_missing")
            assets[field] = raw
            assets[field.replace("_path", "_sha256")] = sha256_file(path)
        normalized[role] = {**copy.deepcopy(dict(required)), **assets}
    core = {
        "schema_version": "study-intake-multi-agent-model-contract-v1",
        "roles": normalized,
        "strict_config": True,
        "ephemeral": True,
        "ignore_user_config": True,
        "requested_service_tier": None,
        "formal_write_count": 0,
    }
    return {**core, "contract_sha256": hashlib.sha256(canonical_bytes(core)).hexdigest()}


def validate_model_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if set(value) != {
        "schema_version", "roles", "strict_config", "ephemeral",
        "ignore_user_config", "requested_service_tier", "formal_write_count",
        "contract_sha256",
    }:
        raise ModelRoleContractError("multi_agent_model_contract_shape_invalid")
    core = {key: copy.deepcopy(value[key]) for key in value if key != "contract_sha256"}
    if (
        value.get("schema_version") != "study-intake-multi-agent-model-contract-v1"
        or value.get("strict_config") is not True
        or value.get("ephemeral") is not True
        or value.get("ignore_user_config") is not True
        or value.get("requested_service_tier") is not None
        or value.get("formal_write_count") != 0
        or value.get("contract_sha256") != hashlib.sha256(canonical_bytes(core)).hexdigest()
    ):
        raise ModelRoleContractError("multi_agent_model_contract_invalid")
    roles = value.get("roles")
    if not isinstance(roles, Mapping) or set(roles) != set(ROLE_SPECS):
        raise ModelRoleContractError("multi_agent_model_roles_invalid")
    for role, required in ROLE_SPECS.items():
        supplied = roles.get(role)
        if not isinstance(supplied, Mapping) or any(supplied.get(key) != expected for key, expected in required.items()):
            raise ModelRoleContractError(f"multi_agent_{role}_contract_invalid")
        for field in ("agent_config_sha256", "tool_policy_sha256"):
            digest = supplied.get(field)
            if not isinstance(digest, str) or len(digest) != 64:
                raise ModelRoleContractError(f"multi_agent_{role}_asset_invalid")
    return copy.deepcopy(dict(value))
