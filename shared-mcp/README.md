# Local Study Read MCP

This project implements a local-only, read-only MCP server for the user's math, 408, and English study repositories.

The v2 runtime exposes exactly five tools:

- `authority_bundle`
- `verify_evidence_batch`
- `math_read_bundle`
- `cs408_read_bundle`
- `english_read_bundle`

It intentionally has no resource, prompt, network, subprocess, writer, model-client, arbitrary-path, arbitrary-SQL, arbitrary-regex, or current-question API. All successful responses report `formal_write_count=0` and `model_call_count=0`.

The capture-bound Luna profile exposes exactly six focused tools in exactly one
subject-only server:

- `get_task_context`
- `read_task_artifact`
- `list_records`
- `get_records`
- `search_records`
- `query_relations`

The three server identities are `kaoyan_math_read`, `kaoyan_cs408_read`, and
`kaoyan_english_read`. A v2 read session binds a content-addressed frozen capture
manifest under `dispatch/luna-capture-freezes`; neither the model nor a tool
argument can supply a local path. The v1 read-session format remains parseable
only for legacy composite service calls. All six focused tools fail with
`CAPTURE_BINDING_REQUIRED` before returning records when the v2 capture binding
is absent.

Math binds formal cards and `知识点库.md`; 408 binds `节点总表.md`,
`知识点标签表.md`, the directed R01-R09 `关系边表.md`, and the canonical review
ledger high-water; English binds the complete 60-record practice-safe corpus,
saved article-learning pages, raw and effective capture events, migration
receipts, master banks, and whole `SP-xxx` pattern-card sections. Raw sources,
formal records, events, projections, candidates, and navigation data have
distinct roles.

Every model-visible item carries `collection`, `data_role`, `parser_version`,
the original `stable_id` and `source_hash`, plus a service-generated canonical
reference of the form `mcp-item:<subject>:<64hex>`. The digest binds the read
generation, collection, stable ID, and source hash; adapters are forbidden from
supplying or overriding it.

Every new-plugin request carries a bounded `route` object containing the caller Skill, plugin version, evidence scope hash, route request ID, and chunk position. The ordinary profile accepts unattributed legacy calls for old-task compatibility. The background profile requires an exact subject-bound `background-<subject>-processing` caller and fails closed before returning evidence when the route is missing or mismatched.

The 408 curation `state.json` source is a projection. It is returned only after
its `event_count` matches the immutable event ledger high-water mark, and both
hashes are included in `projection_event_binding`. Review-event rows likewise
fail closed unless SQLite count, last event, line number, and indexed byte end
match the canonical review JSONL ledger. `morning_sessions` enumerates every
session; it never silently substitutes the latest session.

English `articles` and `sentences` are `raw_source` or
`practice_safe_source`, never formal records. `effective_events` verifies raw
file hashes, canonical raw-object hashes, migration receipts, and canonical
effective-object hashes. Until a deterministic quick-capture projection
manifest is published, an English `projection_bound` check fails closed instead
of echoing an unverified success.

## Recreate the isolated environment

```sh
/opt/miniconda3/bin/python3.13 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
```

## Run tests

```sh
.venv/bin/python -m unittest discover -v
```

## Run the server

```sh
/usr/bin/env -i PATH=/usr/bin:/bin PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 \
  /Users/xiazhibin/Documents/Codex/local-study-read-mcp/.venv/bin/python \
  -m study_read_mcp --stdio --profile ordinary --subjects math,cs408,english
```

For background evidence freezing, start one subject-isolated server with `--profile background --subjects <subject>`. Background servers expose no additional capabilities and require route binding on every call.

For a model-driven frozen capture, use one of the fixed launchers. The launcher
forces the subject and Luna profile; only the v2 manifest path is configurable:

```sh
study-read-mcp-math --stdio --read-session-manifest /absolute/read-session.json
study-read-mcp-cs408 --stdio --read-session-manifest /absolute/read-session.json
study-read-mcp-english --stdio --read-session-manifest /absolute/read-session.json
```

Text artifacts are paged with a session-bound `b3_` cursor. PNG, JPEG, and WebP
artifacts return real MCP `ImageContent` plus the same metadata envelope in
`structuredContent`; base64 and filesystem paths are not included in the
structured envelope.

The `kaoyan_read` Codex MCP configuration was installed globally after explicit approval on 2026-08-07. Versioned Skill routing is distributed through the `kaoyan-study-intake` plugin; old tasks retain their previous tool snapshot.
