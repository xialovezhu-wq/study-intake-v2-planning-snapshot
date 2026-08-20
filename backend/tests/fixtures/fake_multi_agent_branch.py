#!/usr/bin/env python3
"""Hermetic branch/MCP process used only by Multi-Agent V2 tests."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time


def main() -> int:
    payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    request = payload["request"]
    branch = request["branch"]
    constraints = branch.get("query_constraints") or {}
    delay_ms = int(constraints.get("delay_ms") or 0)
    if delay_ms:
        time.sleep(delay_ms / 1000)
    if constraints.get("fail") is True:
        print("fixture branch failure", file=sys.stderr)
        return 7
    branch_id = request["branch_id"]
    session_id = payload["read_session_id"]
    subject = request["subject"]
    source_sha = hashlib.sha256(
        f"{subject}:{branch_id}:{branch['purpose']}".encode("utf-8")
    ).hexdigest()
    calls = [
        {
            "sequence": 1,
            "read_session_id": session_id,
            "branch_id": branch_id,
            "tool": "get_task_context",
            "cursor": None,
            "previous_cursor": None,
        },
        {
            "sequence": 2,
            "read_session_id": session_id,
            "branch_id": branch_id,
            "tool": "read_task_artifact",
            "cursor": f"cursor-{branch_id}-1",
            "previous_cursor": None,
        },
        {
            "sequence": 3,
            "read_session_id": session_id,
            "branch_id": branch_id,
            "tool": "get_records",
            "cursor": None,
            "previous_cursor": f"cursor-{branch_id}-1",
        },
    ]
    output = {
        "parent_agent_id": "terra-parent-fixture",
        "fixture_pid": os.getpid(),
        "fixture_pgid": os.getpgid(0),
        "calls": calls,
        "evidence": [
            {
                "evidence_ref": f"mcp-item:{subject}:{source_sha}",
                "subject": subject,
                "collection": branch["collection_scope"][0] if branch["collection_scope"] else "task",
                "stable_id": f"{branch_id}-item",
                "source_sha256": source_sha,
                "generation": request["generation"],
                "authority_snapshot_sha256": request["authority_snapshot_sha256"],
                "branch_id": branch_id,
                "read_session_id": session_id,
                "summary": branch["purpose"],
            }
        ],
        "findings": [{"kind": "fixture", "summary": branch["purpose"]}],
        "conflicts": [],
        "missing_evidence": [],
        "pagination_closed": True,
        "formal_write_count": 0,
    }
    sys.stdout.write(json.dumps(output, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
