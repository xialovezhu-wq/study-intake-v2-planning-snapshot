"""Versioned parser for parent/leaf Multi-Agent event evidence."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping, Sequence


class MultiAgentEventError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def parse_multi_agent_events(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes)) or not events:
        raise MultiAgentEventError("multi_agent_event_stream_empty")
    seen_ids: set[str] = set()
    children: dict[str, dict[str, Any]] = {}
    parent_id: str | None = None
    last_sequence = 0
    hashes: list[str] = []
    for raw in events:
        if not isinstance(raw, Mapping) or set(raw) != {
            "schema_version", "event_id", "sequence", "event_type", "role",
            "parent_agent_id", "agent_id", "branch_id", "model", "reasoning_effort",
            "status", "payload_sha256", "formal_write_count",
        }:
            raise MultiAgentEventError("multi_agent_event_shape_invalid")
        event = copy.deepcopy(dict(raw))
        event_id = event.get("event_id")
        sequence = event.get("sequence")
        if (
            event.get("schema_version") != "codex_multi_agent_event_fixture_v1"
            or not isinstance(event_id, str) or not event_id or event_id in seen_ids
            or isinstance(sequence, bool) or not isinstance(sequence, int) or sequence != last_sequence + 1
            or event.get("formal_write_count") != 0
        ):
            raise MultiAgentEventError("multi_agent_event_identity_invalid")
        seen_ids.add(event_id)
        last_sequence = sequence
        role = event.get("role")
        event_type = event.get("event_type")
        agent_id = event.get("agent_id")
        if role == "orchestrator":
            if event.get("model") != "gpt-5.6-terra" or event.get("reasoning_effort") != "ultra":
                raise MultiAgentEventError("orchestrator_runtime_identity_invalid")
            parent_id = str(agent_id)
        elif role == "reader":
            if event.get("model") != "gpt-5.6-luna" or event.get("reasoning_effort") != "max":
                raise MultiAgentEventError("reader_runtime_identity_invalid")
            if parent_id is None or event.get("parent_agent_id") != parent_id or not event.get("branch_id"):
                raise MultiAgentEventError("reader_parent_binding_invalid")
            child = children.setdefault(str(agent_id), {"spawned": False, "terminal": False, "branch_id": event["branch_id"]})
            if child["branch_id"] != event["branch_id"]:
                raise MultiAgentEventError("reader_branch_binding_invalid")
            if event_type == "spawn":
                if child["spawned"]:
                    raise MultiAgentEventError("reader_duplicate_spawn")
                child["spawned"] = True
            elif event_type in {"terminal", "cancelled", "timed_out"}:
                if not child["spawned"] or child["terminal"]:
                    raise MultiAgentEventError("reader_terminal_order_invalid")
                child["terminal"] = True
            elif event_type == "spawn_child":
                raise MultiAgentEventError("reader_recursive_delegation_forbidden")
            elif not child["spawned"]:
                raise MultiAgentEventError("reader_event_before_spawn")
        elif role == "critical_reviewer":
            if event.get("model") != "gpt-5.6-terra" or event.get("reasoning_effort") != "ultra":
                raise MultiAgentEventError("reviewer_runtime_identity_invalid")
        else:
            raise MultiAgentEventError("multi_agent_role_invalid")
        hashes.append(sha256_value(event))
    incomplete = sorted(agent_id for agent_id, child in children.items() if not child["terminal"])
    if incomplete:
        raise MultiAgentEventError("reader_terminal_missing")
    core = {
        "schema_version": "multi_agent_event_chain_receipt_v1",
        "parent_agent_id": parent_id,
        "child_agent_ids": sorted(children),
        "branch_ids": sorted(child["branch_id"] for child in children.values()),
        "event_count": len(events),
        "ordered_event_sha256s": hashes,
        "formal_write_count": 0,
    }
    return {**core, "event_chain_sha256": sha256_value(core)}
