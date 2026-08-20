from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping, Sequence

from .errors import StudyReadError


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def query_binding(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


CURSOR_RE = re.compile(r"^c3_([0-9]+)_([a-f0-9]{16})$")
BLOB_CURSOR_RE = re.compile(r"^b3_([0-9]+)_([a-f0-9]{16})$")


def _cursor_tag(
    *, offset: int, query_sha256: str, session_sha256: str, generation: str
) -> str:
    return hashlib.sha256(
        canonical_bytes(
            {
                "offset": offset,
                "query_sha256": query_sha256,
                "session_sha256": session_sha256,
                "generation": generation,
            }
        )
    ).hexdigest()[:16]


def page_records(
    records: Sequence[dict[str, Any]], *, page_size: int, cursor: str | None,
    query_sha256: str, session_sha256: str, generation: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    offset = 0
    if cursor is not None:
        matched = CURSOR_RE.fullmatch(cursor)
        if matched is None:
            raise StudyReadError("CURSOR_INVALID", "pagination cursor is invalid")
        offset = int(matched.group(1))
        expected = _cursor_tag(
            offset=offset,
            query_sha256=query_sha256,
            session_sha256=session_sha256,
            generation=generation,
        )
        if matched.group(2) != expected:
            raise StudyReadError(
                "CURSOR_SESSION_MISMATCH",
                "pagination cursor does not belong to this read session and query",
            )
    total = len(records)
    if offset > total:
        raise StudyReadError("CURSOR_INVALID", "pagination cursor exceeds the result set")
    returned = list(records[offset : offset + page_size])
    next_offset = offset + len(returned)
    next_cursor = None
    if next_offset < total:
        next_cursor = "c3_{}_{}".format(
            next_offset,
            _cursor_tag(
                offset=next_offset,
                query_sha256=query_sha256,
                session_sha256=session_sha256,
                generation=generation,
            ),
        )
    return returned, {
        "total_count": total,
        "returned_count": len(returned),
        "offset": offset,
        "page_size": page_size,
        "next_cursor": next_cursor,
        "truncated": next_cursor is not None,
        "complete": next_cursor is None,
        "query_sha256": query_sha256,
    }


def blob_page(
    data: bytes, *, max_bytes: int, cursor: str | None, query_sha256: str,
    session_sha256: str, generation: str,
) -> tuple[bytes, dict[str, Any]]:
    offset = 0
    if cursor is not None:
        matched = BLOB_CURSOR_RE.fullmatch(cursor)
        if matched is None:
            raise StudyReadError("CURSOR_INVALID", "artifact cursor is invalid")
        offset = int(matched.group(1))
        expected = _cursor_tag(
            offset=offset, query_sha256=query_sha256,
            session_sha256=session_sha256, generation=generation,
        )
        if matched.group(2) != expected:
            raise StudyReadError(
                "CURSOR_SESSION_MISMATCH",
                "artifact cursor does not belong to this read session and artifact",
            )
    if offset > len(data):
        raise StudyReadError("CURSOR_INVALID", "artifact cursor exceeds the byte stream")
    end = min(offset + max_bytes, len(data))
    while end > offset:
        try:
            data[offset:end].decode("utf-8")
            break
        except UnicodeDecodeError:
            end -= 1
    if end == offset and offset < len(data):
        raise StudyReadError("INTERNAL_SAFE", "artifact page cannot preserve UTF-8 boundaries")
    chunk = data[offset:end]
    next_cursor = None
    if end < len(data):
        next_cursor = "b3_{}_{}".format(
            end,
            _cursor_tag(
                offset=end, query_sha256=query_sha256,
                session_sha256=session_sha256, generation=generation,
            ),
        )
    return chunk, {
        "total_count": len(data),
        "returned_count": len(chunk),
        "offset": offset,
        "page_size": max_bytes,
        "next_cursor": next_cursor,
        "truncated": next_cursor is not None,
        "complete": next_cursor is None,
        "query_sha256": query_sha256,
        "unit": "bytes",
    }
