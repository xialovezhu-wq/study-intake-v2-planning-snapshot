---
name: kaoyan-408-daily-intake-curation
description: "408 日终错题编纂：用户明确指定日期并要求集中优化当天快速捕获的错题时，从事实捕获账本冻结精确清单，用高质量语义复核逐题首次正式入库或 redo，保持用户事实不可变，正式写入严格串行，记录每题 receipt 并完成全局验收。泛称提高质量、晚间 D0、当前单题学习或旧 JOB 恢复不使用本 skill。"
---
# Kaoyan 408 Daily Intake Curation
## Outcome
Turn one explicit study date's authorized fact captures into high-quality formal nodes
or redos without making the learner wait or letting a stronger model rewrite immutable facts.
## Entry gate
Read `../_shared/kaoyan-408/runtime-contract.md` and
`../kaoyan-408-wrong-intake/references/fact-capture-schema.md`. Do not load the
personalization harness, historical preferences, cross-question recommendations,
or variant catalogs for this workflow.
Require an explicit `YYYY-MM-DD` or an unambiguous local calendar date. “今天” may be resolved using the current Asia/Shanghai date. Broad “提高质量/回流/同步状态” with
no date or capture IDs stays outside this skill and routes to wrong-intake's read-only scope directory. Evening D0 remains in daily-study-loop.
This skill does not process arbitrary Git changes, receipt UTC dates, file mtimes, chat memory, or topic-similar nodes. It uses only the capture ledger's authorized pending set for the selected study date.
This includes eligible morning-failure captures created by the standing `capture-review-event` policy; it does not rescan the review ledger to invent or supplement the frozen set.
## Freeze the inventory
1. Run ledger `audit`; use `reconcile` only for a real projection drift.
2. Read `status --date DATE` and report pending, unconfirmed, needs-user, curated,
   already-current, and failed counts. If `active_batch` exists, resume that exact
   batch; do not start a second batch.
3. Start one content-addressed batch with a stable idempotency key:
```bash
python3 scripts/intake_fact_capture_408.py --repo . start-batch \
  --date 2026-07-18 --idempotency-key curation:2026-07-18:...
```
4. Freeze exactly the returned capture IDs, capture-set SHA-256, and attempt. A new
same-day capture waits until the active batch closes, then belongs to a later batch;
never silently extend or overlap batches.
If there are zero pending captures, return a zero-write terminal. A capture with
`formalization_authorized=false` remains unconfirmed and is excluded. When the user
later explicitly authorizes that exact capture, append an `authorize` event before
starting a batch; never reinterpret the original capture silently.
For a closed `PARTIAL` item's `needs_user`, never edit A01: after exact canonical user confirmation, call `resolve-needs-user` with its explicit date, capture, formal ID and confirmation event. Identity-only resolution lets the same-set A02 run normal serial apply. Add `--normal-receipt-sha256` only for an existing COMMITTED redo receipt exact-bound to the original batch/capture/evidence and confirmed ID; A02 then performs zero apply, runs `seal-already-current`, and records `already_current`. Any binding or chronology mismatch is zero-write.
## Evidence and field ownership
For each capture, bind its payload hash, stable evidence refs/hashes, study date,
source facts, user facts/provenance, identity hint, and missing fields. Read the
protected primary evidence only as needed to curate; never copy protected content
to the ledger, formal repo, subagent prompts, or visible output.
The shared runtime contract is authoritative for immutable fields and mutable
model-derived fields. A conflict becomes `needs_user`; no vote or stronger wording
may overwrite it, and no guessed learner cause becomes an error tag.
Merge, deletion, source migration, and ID reassignment are separate decisions and
remain zero-write without explicit confirmation.
## High-quality preparation
Resolve exact identity before semantic work. Existing identity is verified first and
selects redo only when the captured evidence requires an update; a new candidate
receives a formal ID only inside the normal formal transaction. Missing
identity, new/redo, user fact, unique main knowledge, or answer-safety evidence that
cannot be resolved from primary evidence becomes `needs_user`.
Independent read-only semantic preparation may run in parallel on frozen,
answer-safe, role-specific views when it improves quality. Follow
`../kaoyan-408-wrong-intake/references/parallel-intake-contract.md`. Do not send
complete stems, answers, options, screenshots, handwriting, or protected paths.
A skill cannot switch its own model. Only claim a stronger model or higher reasoning
effort when execution metadata confirms it. Subagent count is not model evidence.

## Optional one-shot Luna report consumer
After `start-batch` succeeds or an exact `active_batch` is resumed, run once per frozen capture before the first `curate_one`:
```bash
python3 ~/.codex/study-intake-preprocessor/current/bin/preprocess_consumer.py consume --subject cs408 --study-date YYYY-MM-DD --capture-id CAPTURE_ID --batch-id BATCH_ID --capture-set-hash CAPTURE_SET_HASH
```
Take every argument from the frozen batch and bound evidence; never reconstruct it from chat, dashboard state, or package filenames.
The only consumable result is `status=ready` with
`pipeline_status=two_pass_ready`, `model=gpt-5.6-luna`, and
`reasoning_effort=max`. `single_pass_degraded`, `absent`, `stale`, `failed`, and
`disabled` are fail-soft no-report states. The local cs408 adapter is
normally enabled; `disabled` is a deliberate kill switch. Every status has
`formal_write_count=0`.

Never wait, poll, retry, backfill, wake, or invoke Luna. Do not inspect worker state.
The consumer is called exactly once and the chat/session never calls it. A missing,
late, failed, or drifted report does not block Sol from using the frozen raw capture
and direct evidence.

For each item, independently reopen the capture's
`current-question-evidence-bundle-v3` from its private manifest locator. Verify the
manifest hash, bundle object hash, private permissions, source/date/context binding,
the frozen payload, exact `question_mode`, attachment order/role/magic bindings, and
trace v2 disclosure. An image question requires both `question_image` and
`solution_image`; missing either means `evidence_pending`, zero-model, and zero-write.
Legacy captures without bundle v3 are explicitly marked `legacy_evidence`; never
fabricate a v3 binding.

Then use `scripts/sol_curation_decision_408.py seal` to verify the frozen order,
consumer batch/set/payload binding, content-addressed Luna package, private report
JSON, evidence manifest/bundle, quality receipt, processing fingerprint, and report
hashes. Pass `--preprocessor-root ~/.codex/study-intake-preprocessor/current`; Sol must
independently verify that active immutable release and its HMAC authority chain. The
stage receipts must be exactly `analysis` and `critical_review`, both ready with
gpt-5.6-luna and Max. The exact consumer also binds a nonempty `publication_id`,
model, reasoning effort, authority unit/fence, receipt/completion hashes, and release
ID. Sol reopens the HMAC authority record from the verified current release and does
not trust a self-declared `authority_verified` field. Luna drift is quarantined as
`rejected_report`: discard its token and all claims, then continue from raw capture
and reopened evidence. A corrupt raw capture or direct bundle fails closed.

Luna v2 suggestions come only from the independently reopened report JSON's
`formalization_candidates`. Sol must record `adopt`, `modify`, or `reject` for every
claim, including the actual Luna value, actual Sol value, direct evidence refs,
basis, counterevidence, confidence, and unresolved gaps. Evidence refs may point
only to the current raw capture or reopened bundle. Single-pass output is not
consumable. Luna cannot overwrite immutable facts,
resolve identity, allocate an ID, authorize a write, or substitute for Sol.

An immutable conflict, unresolved identity, or material unresolved adopted field
becomes `needs_user` with apply count zero. For `new`, the sealed approved canonical
package keeps `formal_id=null` and `auto_id=true`; only the formal RepoLock allocates
the ID. For `redo`, it must bind one verified existing formal ID.
## Serial formal publication
Process every frozen capture to a terminal result. For one capture at a time:
1. seal the Sol decision and use only its content-addressed approved canonical
   package; a `needs_user` decision performs no formal preflight or apply;
2. hand that package to `kaoyan-408-wrong-intake` as `curate_one`, then run exact
   identity, formal preflight, one batch-size-1 apply, full postcommit, audits, and
   related review;
3. for a new item, preserve `formal_id=null` and `auto_id=true` until the formal
   RepoLock allocates the ID; for a redo, require the sealed existing formal ID;
4. never start the next formal write before this item reaches formal complete,
   `already_current`, `needs_user`, or receipt-bound recoverable failure;
5. after complete, SHA-256 the normal receipt and record `curated`:
```bash
python3 scripts/intake_fact_capture_408.py --repo . mark-result \
  --batch-id BATCH --capture-id CAPTURE --outcome curated \
  --formal-id OS_2020_001 --receipt-sha256 HASH \
  --idempotency-key curation-result:...
```
If exact identity and full validation prove the formal node is already current, record
`already_current` with the formal ID and a verification SHA-256; do not invent a
receipt or replay an apply.
The verification sidecar must bind the exact batch, capture, payload, direct
evidence, decision, formal ID, and repository state; it is not a personalization
artifact.
```bash
python3 scripts/sol_curation_decision_408.py --repo . seal-already-current \
  --decision-sha256 SOL_DECISION_SHA --formal-id FORMAL_ID
```
For an immutable fact gap record `needs_user` with one answer-safe reason. For a
tool or receipt-bound failure record `failed`; preserve the receipt and never replay
a successful formal apply. Continue with independent later captures when safe.

After `mark-result`, run `sol_curation_decision_408.py terminal-binding`. It must
reopen the exact raw JSONL result line, verify its line SHA-256, and for `curated`
verify the committed formal receipt and WAL against the sealed package. When the
one-shot consumer supplied a valid adoption token, call the external
`record-adoption` v2 exactly once with that verified terminal binding. Adoption
observability never authorizes, delays, rolls back, or replays a formal write.
```bash
python3 ~/.codex/study-intake-preprocessor/current/bin/preprocess_consumer.py record-adoption \
  --adoption-token TOKEN --outcome DERIVED_OUTCOME --reason-code SAFE_REASON \
  --sol-decision-sha256 SOL_SHA --canonical-package-sha256 PACKAGE_SHA \
  --item-result-event-id EVENT_ID --item-result-event-sha256 EXACT_LINE_SHA \
  --terminal-outcome curated --formal-id FORMAL_ID \
  --normal-receipt-sha256 RECEIPT_SHA
```
Omit terminal-forbidden fields: `already_current` uses `--verification-sha256`
instead of package/normal receipt; `needs_user` and `failed` carry no formal-success
fields.
## Batch closeout
After every capture has a recorded result, run the capture-ledger audit and seal the
formal repository's required aggregate validation. Full per-item closeout remains the
source of each formal completion; a PASS batch close must bind the returned receipt.
```bash
python3 scripts/intake_fact_capture_408.py --repo . seal-global-audit --batch-id BATCH
python3 scripts/intake_fact_capture_408.py --repo . close-batch --batch-id BATCH \
  --global-closeout pass --global-audit-receipt-sha256 RECEIPT_SHA --idempotency-key curation-close:...
```
After and only after that command proves the named batch is closed with
`global_closeout=pass`, enqueue the next local study date's content-addressed frozen
pack job. This writes only rebuildable sidecars and does not start a session:
```bash
python3 scripts/prepare_next_morning_408.py --repo . prepare-next-morning \
  --date NEXT_LOCAL_STUDY_DATE --trigger nightly_closeout --batch-id BATCH --ensure-queue
```
Keep the returned job ID and the minimal queue/session preparation receipts needed
to start the next morning review. Do not rebuild personalization, cross-question,
variant, recommendation, or preference projections as part of closeout. A pack
preparation failure remains a retryable derived failure and never reopens or replays
the closed formal batch.
Do not complete a batch while an item lacks a result; report successful, needs-user and failed IDs separately. Same-key operations stay idempotent. After close, A02/A03 preserves the full inventory and set hash, carries prior successes from exact result events without reapply, and processes only `failed` or `needs_user_resolved`; unresolved `needs_user` blocks same-set retry. Never reconstruct the set from memory.
## Output
Use `references/output-template.md`. Report date, set hash, result counts, formal IDs, receipt or verification hashes, immutable conflicts, and aggregate audit status. Exclude protected or answer-bearing content.
Stop at a complete or partial audited batch, zero pending captures, or a clearly
named date/permission blocker.
managed_next_pack_route_v1=global_closeout:pass;command:prepare-next-morning

## Local read-only MCP preflight

Eligibility is true only for the read-only inventory and cold evidence preflight: the selected date's curation inventory, named formal-node summaries, direct edges, and required review identities. When eligible, the only allowed MCP binding is the 408-only server `kaoyan_cs408_read`, tool `cs408_read_bundle`, exposed in Codex as `mcp__kaoyan_cs408_read__cs408_read_bundle`; use that fully qualified tool for every chunk. Global namespaces `mcp__kaoyan_read__cs408_read_bundle`, `mcp__kaoyan_read_v2__cs408_read_bundle`, and `mcp__kaoyan_read_v3__cs408_read_bundle` are legacy and must not be called. The math and English namespaces are cross-subject and must not be called. Audit, reconcile, status, start-batch, private evidence, Luna consumer, Sol seal/apply, mark-result, close-batch, receipt, and next-morning preparation remain canonical terminal-only operations.

Every MCP request must use `schema_version=study-read-mcp.v3` and route context for caller `kaoyan-408-daily-intake-curation`, Skill version `2.1.0`, the installed plugin version, a unique route request ID, one canonical evidence-scope hash, ordered chunk metadata, and `consumed_duplicate_read_count=0`. Split more than 24 stable IDs deterministically. Every chunk must bind one generation, authority fingerprint, projection SHA, event-ledger SHA, event high-water count, and last event ID. A missing or inconsistent projection/event binding fails the complete route; `state.json` is a projection and never an event.

Before the first eligible call, check the callable task snapshot for the exact required tool `mcp__kaoyan_cs408_read__cs408_read_bundle`. If that exact tool is absent, legacy or cross-subject namespaces do not satisfy the requirement: call no MCP tool, record `read_route=terminal_fallback`, `required_mcp_tool=mcp__kaoyan_cs408_read__cs408_read_bundle`, `fallback_reason=tool_snapshot_missing`, `chunk_index=0`, and `chunk_count=0`, then immediately run the whole canonical preflight without probing, retrying, restarting Codex, or treating absence as a curation failure. On any other missing evidence, failure, drift, mismatch, timeout, busy, corrupt, or limit response from the required 408 tool, discard all MCP chunks and rebind the complete preflight to one fresh canonical terminal generation. Never mix MCP and fallback data.

After MCP success, consumed terminal duplicate reads for the MCP-covered cold preflight evidence must be zero. Every canonical write transaction must still independently reopen and revalidate its sources; that writer-side authority check is not a duplicate preflight read. MCP never starts or extends a batch, calls Luna, allocates an ID, applies content, marks results, closes a batch, or authorizes a write, and it must report `formal_write_count=0` and `model_call_count=0`.
