# Shared Multi-Agent Processing Contract v4

## Authority layers

1. `fact_capture_v1` stores immutable task facts, original dialogue, attachment hashes, user signals, and source identity. It creates no formal study object.
2. `study-read-mcp-capture-manifest.v1` stores the exact task artifact index. `capture_freeze_receipt_v2` HMAC-binds that manifest and its ordered artifact IDs.
3. `orchestration_read_plan_v1` binds one frozen task, one Terra parent, any number of logical read branches, one release, one generation and one authority snapshot.
4. Every Luna read branch owns one independent child context, `study-read-mcp-read-session.v4` manifest, subject-scoped STDIO launcher, transcript and result receipt.
5. The Host fans all terminal branch results into one content-addressed read bundle. Terra integrates the candidate and a fresh-context Terra reviewer returns `accepted`, `corrected`, `issues_found`, or `technical_quarantine`.
6. Sol is the only formal writer. It requires a complete subject batch, explicit user authorization, independent canonical evidence review, the global FIFO writer lease, and the subject writer lock.

## Task-bound direct MCP route

The Terra parent receives only frozen task artifacts and release-bound orchestration assets. It has no subject-library MCP. Each Luna branch receives only its validated branch request, allowed task artifacts and one private read-session manifest.

For each branch, the Host dynamically starts exactly one independent subject launcher:

- math: `study-read-mcp-math --stdio --read-session-manifest <absolute-v4-json>` as `kaoyan_math_read`;
- cs408: `study-read-mcp-cs408 --stdio --read-session-manifest <absolute-v4-json>` as `kaoyan_cs408_read`;
- english: `study-read-mcp-english --stdio --read-session-manifest <absolute-v4-json>` as `kaoyan_english_read`.

No two branches share a launcher, STDIO transport, cursor or session. The Luna tool snapshot contains exactly `get_task_context`, `read_task_artifact`, `list_records`, `get_records`, `search_records`, and `query_relations`. It contains no delegation, terminal, shell, arbitrary path, SQL, web, plugin, cross-subject, ordinary-bundle, or writer tool.

Each branch calls `get_task_context`, reads every artifact allowed to that branch, and keeps cursor/search-to-get dependencies serial. Different dependency-free branches may execute concurrently. The Terra parent waits for every required branch terminal and never treats a failed or missing branch as covered.

Images arrive only as MCP ImageContent. Paginated text artifacts, including a canonical-source copy of a math solution when no authentic solution image exists, arrive through `read_task_artifact`. Before freezing a canonical-source text copy, the Host verifies a regular UTF-8 non-empty bounded file against its absolute source path and SHA-256, then removes the local path from the model-visible artifact and retains only the source hash plus `source_path_redacted=true`. Structured results and model prompts contain no base64 image bytes or local filesystem path. Every returned item has a service-generated `mcp-item:<subject>:<sha256>` evidence ref bound to the collection, stable ID, source hash, generation, and authority.

A missing tool, unread artifact page, unresolved cursor, duplicate consumed read, cross-subject call, generation drift, projection/event mismatch, output limit, permission error, or ungrounded claim fails the stage closed. Background processing has no terminal fallback, and a failed generation cannot append an old package.

## Multi-agent quality closure

Model calls are role- and branch-count dependent rather than fixed at two:

1. One Terra orchestrator creates and validates the read plan.
2. Every logical branch runs once as an independent Luna leaf; excess branches wait in fair waves and are never dropped.
3. Terra integrates the complete read bundle.
4. One fresh-context Terra reviewer audits the frozen bundle and candidate without automatically reopening the database.

`accepted` publishes the original proposal. `corrected` preserves both original and revised candidates. `issues_found` remains execution success with `sol_review_ready=true`, `quality_clean=false`, preserved candidate and risk report, and no automatic retry. `technical_quarantine` preserves raw diagnostics but exposes no trusted candidate.

Every task, branch, bundle, review and handoff has `formal_write_count=0`. Runtime model identity remains `requested_unverified` unless independently attested.

## Allowed Luna operations

- `mark_existing_item`
- `propose_item_update`
- `propose_new_item`
- `mark_existing_knowledge`
- `propose_new_knowledge`
- `propose_relation`
- `mark_status`
- `propose_status_change`
- `needs_review`

Reject claims that a formal object was created, updated, linked, mastered, removed, or committed. Luna never allocates a formal ID, performs a formal relation change, updates mastery, calls Sol, or authorizes a write.

## Subject batches and parallel Luna execution

Math, 408, and English have independent Dispatchers, queues, read sessions, generations, and `subject_luna_batch_v1` documents. Distinct frozen tasks may run concurrently across subjects and within one subject. Only submissions with the same capture identity and byte-identical frozen payload are idempotently deduplicated; there is no fixed application-level Luna concurrency cap.

`all_terminal=true` means every frozen task has reached a terminal state. `sol_review_ready` and `quality_clean` are independent. An `issues_found` item remains visible to Sol and does not block clean siblings; quarantine remains available as a diagnostic item.

A subject may hand off to Sol as soon as that subject is `sol_ready`; it does not wait for the other subjects. New captures for a subject under formal apply enter the next subject batch and cannot open a session against the committed generation.

## Explicit Sol authorization and global serialization

`daily_sol_batch_v2` binds the complete ready subject batch, all proposal/package/quality hashes, one explicit user authorization receipt, and the subject writer adapter. Readiness alone never enqueues or starts a write.

Authorized batches enter `global_sol_writer_lease_v1` FIFO by authorization receipt time. Lock order is always global Sol writer lease first, then the subject writer lock. `active_writer_count` is at most one; a second claim fails closed and leaves a receipt. Sol independently reopens canonical facts and formal state, records `adopt`, `modify`, or `reject` in `sol_review_receipt_v1`, and only then may issue `sol_commit_receipt_v1` for deterministic apply.

When Sol holds the lease for one subject, the other two subjects' Luna Dispatchers, MCP sessions, model calls, and heartbeats continue. Only the committing subject freezes its completed batch and generation. A Sol failure enters `safe_paused`, produces a failure or rollback receipt before lease release, and does not pause other-subject Luna execution.

## Dashboard projection

`study-intake-dashboard-projection-v3` is GET-only and keeps Luna events separate from Sol events. It shows per-subject queued, Analysis, Critical Review, terminal, quality-passed, needs-rework, failed, evidence-pending, `all_terminal`, `sol_ready`, and handoff state. The global Sol panel shows the FIFO queue, active subject, batch, item, fencing token, reviewed count, committed count, remaining count, and formal write count.

Historical heartbeats are labeled `previous_release_heartbeat` and never contribute to readiness. `sol_reviewed` never means `sol_committed`. The projection exposes no full sensitive task prose and cannot trigger a model call, authorization, or write.

The separately bound Sol infrastructure preflight is not a processing branch. It
uses no Capture, Provider, Terra, Luna, candidate, queue, lease or handoff. Its four
library tools are read-only, its receipts are explicitly non-production evidence,
and it cannot satisfy any live task or production-acceptance gate.
