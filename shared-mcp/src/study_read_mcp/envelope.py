from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from . import SCHEMA_VERSION
from .errors import StudyReadError
from .release import SERVER_RELEASE
from .safeio import json_size


def success_envelope(
    *, subject: str, data_role: str, authority_source_id: str, generation: str,
    authority_fingerprint: str, items: list[dict[str, Any]], warnings: list[str] | None = None,
    adapter_release: str = SERVER_RELEASE, preprocessor_release: str | None = None,
    consistency: str = "bound_snapshot", output_limit: int = 98_304,
    route_context: dict[str, Any] | None = None, profile: str = "ordinary",
    page: dict[str, Any] | None = None, read_session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    read_route = route_context or {
        "caller_skill_id": "unattributed-legacy",
        "caller_skill_version": "legacy",
        "plugin_version": "legacy",
        "route_request_id": "legacy-unattributed",
        "evidence_scope_hash": "0" * 64,
        "read_route": "mcp",
        "chunk_index": 1,
        "chunk_count": 1,
        "consumed_duplicate_read_count": 0,
    }
    value = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "server_release": SERVER_RELEASE,
        "adapter_release": adapter_release,
        "subject": subject,
        "data_role": data_role,
        "authority_source_id": authority_source_id,
        "generation": generation,
        "preprocessor_release": preprocessor_release,
        "authority_fingerprint": authority_fingerprint,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "consistency": consistency,
        "profile": profile,
        "read_route": read_route,
        "warnings": warnings or [],
        "items": items,
        "formal_write_count": 0,
        "model_call_count": 0,
        "mcp_tool_call_count": 0,
    }
    if page is not None:
        value.update(page)
    if read_session is not None:
        value["read_session"] = read_session
    if json_size(value) > output_limit:
        raise StudyReadError("OUTPUT_LIMIT", "response exceeds the fixed output limit")
    return value


def error_envelope(
    tool: str, error: StudyReadError, request_id: str | None = None,
    route_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    opaque = request_id or hashlib.sha256(f"{tool}:{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16]
    return {
        "ok": False,
        "schema_version": SCHEMA_VERSION,
        "server_release": SERVER_RELEASE,
        "request_id": opaque,
        "tool": tool,
        "read_route": route_context,
        "error": {"code": error.code, "message": error.message, "retryable": error.retryable},
        "formal_write_count": 0,
        "model_call_count": 0,
        "mcp_tool_call_count": 0,
    }


def assert_expected(
    *, generation: str, release: str, fingerprint: str,
    expected_generation: str | None = None, expected_release: str | None = None,
    expected_fingerprint: str | None = None,
) -> None:
    if expected_generation is not None and expected_generation != generation:
        raise StudyReadError("GENERATION_MISMATCH", "authority generation does not match expectation", True)
    if expected_release is not None and expected_release != release:
        raise StudyReadError("RELEASE_MISMATCH", "adapter release does not match expectation")
    if expected_fingerprint is not None and expected_fingerprint != fingerprint:
        raise StudyReadError("AUTHORITY_DRIFT", "authority fingerprint does not match expectation", True)


def fingerprint_object(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
