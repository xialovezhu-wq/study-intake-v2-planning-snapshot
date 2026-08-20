from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import date
from typing import Any, Callable, Literal

from mcp.server.fastmcp import FastMCP, Image
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .envelope import error_envelope
from .errors import StudyReadError
from .models import RouteContext
from .safeio import json_size
from .service import StudyReadService


READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)


class FocusedToolOutput(BaseModel):
    """Stable common output schema; operation-specific fields remain explicit extras."""

    model_config = ConfigDict(extra="allow")
    ok: bool
    schema_version: str
    subject: str | None = None
    data_role: str | None = None
    generation: str | None = None
    authority_fingerprint: str | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)
    read_session: dict[str, Any] | None = None
    total_count: int | None = None
    returned_count: int | None = None
    offset: int | None = None
    page_size: int | None = None
    next_cursor: str | None = None
    truncated: bool | None = None
    complete: bool | None = None
    query_sha256: str | None = None
    error: dict[str, Any] | None = None
    formal_write_count: Literal[0]
    model_call_count: Literal[0]
    mcp_tool_call_count: int


def _log(tool: str, request_id: str, status: str, started: float, result: dict[str, Any]) -> None:
    route = result.get("read_route") if isinstance(result.get("read_route"), dict) else {}
    record = {
        "request_id": request_id, "tool": tool, "status": status,
        "subject": result.get("subject"),
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
        "item_count": len(result.get("items", [])), "return_bytes": json_size(result),
        "content_return_bytes": result.get("content_return_bytes", 0),
        "generation": result.get("generation"), "fingerprint": result.get("authority_fingerprint"),
        "profile": result.get("profile"),
        "caller_skill_id": route.get("caller_skill_id"),
        "caller_skill_version": route.get("caller_skill_version"),
        "plugin_version": route.get("plugin_version"),
        "route_request_id": route.get("route_request_id"),
        "evidence_scope_hash": route.get("evidence_scope_hash"),
        "read_route": route.get("read_route"),
        "chunk_index": route.get("chunk_index"),
        "chunk_count": route.get("chunk_count"),
        "consumed_duplicate_read_count": route.get("consumed_duplicate_read_count"),
    }
    sys.stderr.write(json.dumps(record, separators=(",", ":")) + "\n")
    sys.stderr.flush()


def _safe_call(
    tool: str, raw_route: dict[str, Any] | None,
    operation: Callable[[RouteContext | None], dict[str, Any]],
) -> dict[str, Any]:
    route_context: RouteContext | None = None
    request_id = uuid.uuid4().hex[:16]
    started = time.monotonic()
    try:
        if raw_route is not None:
            try:
                route_context = RouteContext.model_validate(raw_route)
            except ValidationError as exc:
                raise StudyReadError("INVALID_ARGUMENT", "read route context is invalid") from exc
            request_id = route_context.route_request_id
        result = operation(route_context)
        _log(tool, request_id, "OK", started, result)
        return result
    except StudyReadError as exc:
        route = route_context.model_dump(mode="json") if route_context is not None else raw_route
        result = error_envelope(tool, exc, request_id, route)
        _log(tool, request_id, exc.code, started, result)
        return result
    except Exception:
        route = route_context.model_dump(mode="json") if route_context is not None else raw_route
        result = error_envelope(
            tool, StudyReadError("INTERNAL_SAFE", "internal read failed safely"), request_id, route
        )
        _log(tool, request_id, "INTERNAL_SAFE", started, result)
        return result


def _safe_artifact_call(
    tool: str, operation: Callable[[], tuple[dict[str, Any], Any]],
) -> CallToolResult:
    request_id = uuid.uuid4().hex[:16]
    started = time.monotonic()
    try:
        result, artifact_page = operation()
        content: list[Any] = [
            TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            )
        ]
        if artifact_page.image_bytes is not None:
            image_format = {
                "image/png": "png", "image/jpeg": "jpeg", "image/webp": "webp",
            }.get(artifact_page.image_media_type)
            if image_format is None:
                raise StudyReadError(
                    "ARTIFACT_MEDIA_UNSUPPORTED", "image media type is not supported by MCP"
                )
            content.append(
                Image(data=artifact_page.image_bytes, format=image_format).to_image_content()
            )
        _log(tool, request_id, "OK", started, result)
        return CallToolResult(content=content, structuredContent=result, isError=False)
    except StudyReadError as exc:
        result = error_envelope(tool, exc, request_id, None)
        _log(tool, request_id, exc.code, started, result)
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, ensure_ascii=False, separators=(",", ":")))],
            structuredContent=result,
            isError=True,
        )
    except Exception:
        result = error_envelope(
            tool, StudyReadError("INTERNAL_SAFE", "internal read failed safely"), request_id, None
        )
        _log(tool, request_id, "INTERNAL_SAFE", started, result)
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, ensure_ascii=False, separators=(",", ":")))],
            structuredContent=result,
            isError=True,
        )


def build_server(service: StudyReadService) -> FastMCP:
    server_name = (
        f"kaoyan_{next(iter(service.subjects))}_read"
        if len(service.subjects) == 1 else "kaoyan_read"
    )
    mcp = FastMCP(
        server_name,
        instructions=(
            "Local-only and strictly read-only study evidence. In Luna profile the model "
            "chooses collections, queries, IDs, and cursor continuation inside one subject-bound "
            "read session. Follow next_cursor until complete whenever a claim depends on full "
            "coverage. Never mix generations and never use shell, arbitrary paths, SQL, network, "
            "or any write operation."
        ),
    )

    if service.profile == "infrastructure_preflight":
        subject = next(iter(service.subjects))

        @mcp.tool(
            name="list_records",
            description=(
                "Infrastructure-only read of one allowlisted subject collection. "
                "The result is non-candidate and non-production evidence."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def preflight_list_records(
            collection: str,
            cursor: str | None = None,
            page_size: int = 5,
            article_id: str | None = None,
            study_date: date | None = None,
        ) -> FocusedToolOutput:
            return _safe_call(
                "list_records",
                None,
                lambda _context: service.list_records(
                    subject,
                    collection,
                    cursor,
                    page_size,
                    article_id,
                    study_date,
                ),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="get_records",
            description=(
                "Infrastructure-only exact-ID read from one allowlisted subject "
                "collection. No mutation or candidate is possible."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def preflight_get_records(
            collection: str,
            ids: list[str],
            cursor: str | None = None,
            page_size: int = 5,
        ) -> FocusedToolOutput:
            return _safe_call(
                "get_records",
                None,
                lambda _context: service.get_records(
                    subject, collection, ids, cursor, page_size
                ),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="search_records",
            description=(
                "Infrastructure-only bounded subject search. Follow an opaque cursor "
                "without changing other arguments."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def preflight_search_records(
            query: str,
            cursor: str | None = None,
            page_size: int = 5,
        ) -> FocusedToolOutput:
            return _safe_call(
                "search_records",
                None,
                lambda _context: service.search_records(
                    subject, query, cursor, page_size
                ),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="query_relations",
            description=(
                "Infrastructure-only direct-relation read for exact endpoint IDs. "
                "No relation mutation is exposed."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def preflight_query_relations(
            ids: list[str],
            relation_types: list[str] | None = None,
            include_candidates: bool = False,
            cursor: str | None = None,
            page_size: int = 5,
        ) -> FocusedToolOutput:
            return _safe_call(
                "query_relations",
                None,
                lambda _context: service.query_relations(
                    subject,
                    ids,
                    relation_types,
                    include_candidates,
                    cursor,
                    page_size,
                ),
            )  # type: ignore[return-value]

        return mcp

    if service.profile == "luna":
        subject = next(iter(service.subjects))

        @mcp.tool(
            name="get_task_context",
            description=(
                "Read only the immutable task identity and artifact index for this subject-bound "
                "capture. It never returns task prose and never exposes local paths."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def get_task_context() -> FocusedToolOutput:
            return _safe_call(
                "get_task_context", None,
                lambda _context: service.get_task_context(subject),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="read_task_artifact",
            description=(
                "Read one artifact bound by artifact_id in get_task_context. Text is paged by "
                "opaque cursor. Question and solution images return real MCP ImageContent; image "
                "bytes and filesystem paths are never placed in structuredContent."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def read_task_artifact(
            artifact_id: str, cursor: str | None = None, max_bytes: int = 16_384,
        ) -> FocusedToolOutput:
            return _safe_artifact_call(
                "read_task_artifact",
                lambda: service.read_task_artifact(subject, artifact_id, cursor, max_bytes),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="list_records",
            description=(
                "Enumerate one allowlisted subject collection with deterministic complete "
                "pagination. Follow next_cursor exactly until complete."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def list_records(
            collection: str, cursor: str | None = None, page_size: int = 24,
            article_id: str | None = None, study_date: date | None = None,
        ) -> FocusedToolOutput:
            return _safe_call(
                "list_records", None,
                lambda _context: service.list_records(
                    subject, collection, cursor, page_size, article_id, study_date
                ),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="get_records",
            description=(
                "Read exact stable IDs from one allowlisted subject collection. It performs no "
                "host ranking and never crosses the subject boundary."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def get_records(
            collection: str, ids: list[str], cursor: str | None = None,
            page_size: int = 24,
        ) -> FocusedToolOutput:
            return _safe_call(
                "get_records", None,
                lambda _context: service.get_records(
                    subject, collection, ids, cursor, page_size
                ),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="search_records",
            description=(
                "Search every allowlisted semantic record in this subject without hidden top-k. "
                "Follow the deterministic cursor until complete when coverage matters."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def search_records(
            query: str, cursor: str | None = None, page_size: int = 24,
        ) -> FocusedToolOutput:
            return _safe_call(
                "search_records", None,
                lambda _context: service.search_records(subject, query, cursor, page_size),
            )  # type: ignore[return-value]

        @mcp.tool(
            name="query_relations",
            description=(
                "Read direct relations touching exact endpoint IDs. Formal declarations are the "
                "default; math candidates require include_candidates=true and remain labeled."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def query_relations(
            ids: list[str], relation_types: list[str] | None = None,
            include_candidates: bool = False, cursor: str | None = None,
            page_size: int = 24,
        ) -> FocusedToolOutput:
            return _safe_call(
                "query_relations", None,
                lambda _context: service.query_relations(
                    subject, ids, relation_types, include_candidates, cursor, page_size
                ),
            )  # type: ignore[return-value]

        return mcp

    if service.profile == "morning_preparation":
        @mcp.tool(
            name="cs408_morning_preparation_bundle",
            description=(
                "Read protected, exact, queue-bound evidence for one contiguous chunk of at most "
                "four 408 morning-review items. This isolated tool performs no model calls or writes."
            ),
            annotations=READ_ONLY,
            structured_output=True,
        )
        def cs408_morning_preparation_bundle(
            request: dict[str, Any],
            expected_generation: str | None = None,
            expected_release: str | None = None,
            expected_fingerprint: str | None = None,
            route: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return _safe_call(
                "cs408_morning_preparation_bundle",
                route,
                lambda context: service.cs408_morning_preparation_bundle(
                    request,
                    route_context=context,
                    expected_generation=expected_generation,
                    expected_release=expected_release,
                    expected_fingerprint=expected_fingerprint,
                ),
            )

        return mcp

    @mcp.tool(name="authority_bundle", description="Bind selected study capabilities to current authority generations and fingerprints.", annotations=READ_ONLY, structured_output=True)
    def authority_bundle(
        subjects: list[str], capabilities: list[str], expected_generation: dict[str, str] | None = None,
        expected_release: dict[str, str] | None = None, expected_fingerprint: dict[str, str] | None = None,
        checks: list[str] | None = None, route: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _safe_call(
            "authority_bundle", route,
            lambda context: service.authority_bundle(
                subjects, capabilities, expected_generation, expected_release,
                expected_fingerprint, checks, context,
            ),
        )

    @mcp.tool(name="verify_evidence_batch", description="Verify a bounded batch of allowlisted stable artifacts by content hash.", annotations=READ_ONLY, structured_output=True)
    def verify_evidence_batch(
        items: list[dict[str, Any]], expected_generation: dict[str, str] | None = None,
        expected_release: dict[str, str] | None = None, route: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _safe_call(
            "verify_evidence_batch", route,
            lambda context: service.verify_evidence_batch(
                items, expected_generation, expected_release, context
            ),
        )

    if "math" in service.subjects:
        @mcp.tool(name="math_read_bundle", description="Read bounded formal-card summaries, activity evidence, direct relations, or review snapshots. Never use for the current-question or capture hot path.", annotations=READ_ONLY, structured_output=True)
        def math_read_bundle(
            queries: list[dict[str, Any]], expected_generation: str | None = None,
            expected_release: str | None = None, expected_fingerprint: str | None = None,
            route: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return _safe_call(
                "math_read_bundle", route,
                lambda context: service.math_read_bundle(
                    queries, route_context=context, expected_generation=expected_generation,
                    expected_release=expected_release, expected_fingerprint=expected_fingerprint,
                ),
            )

    if "cs408" in service.subjects:
        @mcp.tool(name="cs408_read_bundle", description="Read bounded 408 curation inventory, formal-node summaries, direct edges, morning metadata, or review identities. Never returns current question or answers.", annotations=READ_ONLY, structured_output=True)
        def cs408_read_bundle(
            queries: list[dict[str, Any]], expected_generation: str | None = None,
            expected_release: str | None = None, expected_fingerprint: str | None = None,
            route: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return _safe_call(
                "cs408_read_bundle", route,
                lambda context: service.cs408_read_bundle(
                    queries, route_context=context, expected_generation=expected_generation,
                    expected_release=expected_release, expected_fingerprint=expected_fingerprint,
                ),
            )

    if "english" in service.subjects:
        @mcp.tool(name="english_read_bundle", description="Read one bounded English article business bundle. Questions, answers, explanations, and protected sources are excluded.", annotations=READ_ONLY, structured_output=True)
        def english_read_bundle(
            request: dict[str, Any], route: dict[str, Any] | None = None
        ) -> dict[str, Any]:
            return _safe_call(
                "english_read_bundle", route,
                lambda context: service.english_read_bundle(request, route_context=context),
            )

    return mcp
