# MCP Read Route Contract v4

## Ordinary interactive Skills

Every ordinary call supplies `caller_skill_id`, `caller_skill_version`, `plugin_version`, `route_request_id`, `evidence_scope_hash`, `required_mcp_tool`, `read_route`, ordered chunk metadata, nullable `fallback_reason`, and `consumed_duplicate_read_count=0`.

Each Skill is pinned to one subject-only namespace and bundle:

- math Skills: `mcp__kaoyan_math_read__math_read_bundle`;
- 408 Skill: `mcp__kaoyan_cs408_read__cs408_read_bundle`;
- English Skills: `mcp__kaoyan_english_read__english_read_bundle`.

Global `kaoyan_read`, `kaoyan_read_v2`, and `kaoyan_read_v3` namespaces are legacy. A Skill must not call a legacy namespace or another subject namespace even when it is visible in the task snapshot.

When eligible, use the exact subject tool first. Split more than 24 stable IDs deterministically and bind every chunk to one generation and authority fingerprint. One failed or mismatched chunk invalidates the whole route. If the exact tool is absent from an old task snapshot, call no MCP tool and record `read_route=terminal_fallback`, `fallback_reason=tool_snapshot_missing`, `chunk_index=0`, and `chunk_count=0`. Rebind the whole canonical scope to one fresh terminal generation; never mix MCP and terminal evidence.

After MCP success, consumed terminal duplicate reads for MCP-covered cold evidence must be zero. Terminal-only business commands, private evidence, rules, visual sources, and independent writer revalidation remain on their canonical routes.

## Background Multi-Agent read branches

Background tasks never use the static ordinary bundle configuration. Terra creates a validated read plan but has no subject-library MCP. For every logical branch the Host dynamically starts a separate Luna leaf and exact subject executable with a distinct absolute private `study-read-mcp-read-session.v4` manifest and immutable authority snapshot v2. Its tool surface is exactly `get_task_context`, `read_task_artifact`, `list_records`, `get_records`, `search_records`, and `query_relations`.

Calls with response dependencies remain serial inside one branch. Different dependency-free branches run concurrently and never share STDIO, session or cursor state. Fresh-context Terra review reads the frozen read bundle and candidate by default and does not automatically start another library session. Background MCP has no terminal fallback. Subject mismatch, missing artifact, cursor tamper, pagination gap, generation drift, authority drift, duplicate consumed read, projection/event mismatch, timeout, or output limit is preserved in the branch result and cannot be hidden during fan-in.

All routes remain read-only. Ordinary and background MCP return `formal_write_count=0`; neither route authorizes Luna or Sol formal writes.

## Sol infrastructure preflight

The current root Sol may use the sealed `preflight-server` launcher only for
`purpose=infrastructure_preflight`. This route is capture-free and task-free: its
private session has `capture_id_absent=true`,
`production_task_id_absent=true`, `candidate_eligible=false`,
`production_evidence=false`, `formal_write_allowed=false`, and
`formal_write_count=0`.

The server exposes exactly `list_records`, `get_records`, `search_records`, and
`query_relations`. `tools/list` must mark every tool read-only and expose no create,
update, delete, upsert, commit, import, migrate, authorization, promotion, or writer
operation. One session belongs to one subject, one PID/PGID, one MCP release, one
tool-policy hash, and one production read-root identity. The harness performs one
minimal read and at most one cursor continuation, closes STDIO, verifies exact
process cleanup, and publishes content-addressed call and terminal receipts.

This preflight proves launcher, protocol, allowlist, Schema, read-root and cleanup
health only. It never proves a Luna branch identity, a real Capture task, production
concurrency, candidate quality, or production acceptance.
