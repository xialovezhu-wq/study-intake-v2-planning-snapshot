---
name: kaoyan-math-nightly-qa
description: "End-of-day high-quality batch closeout for math mistakes captured during study. Use for 夜间质检, 睡前集中优化, 高质量改写今天所有入库错题, 仔细审核今天快速记录的错题, or equivalent requests. Derive the authoritative cross-task target set from pending events in 数学一回滚复习系统/快速入库事件.jsonl, preserve those raw facts immutably, resolve identity and freeze the exact evidence before formal writes, group multiple events per real question, rewrite each formal card once, run one final wrongnet rebuild, complete targeted rollback and SHADOW relationship proposals, synchronize the required Wiki layer, and append a structured closeout only after built-in verification passes. Failed items remain pending and retryable. Do not use for active-study single-question capture, historical full-library cleanup, weak-edge backlog, or old-question selection."
---

# Kaoyan Math Nightly QA

## Outcome

Turn the day's fast evidence events into high-quality formal cards and verified derived layers without blocking the learning session that produced them.

The immutable capture ledger is the fact layer. Nightly QA may improve structure, wording, taxonomy, and projections, but it must not rewrite what the user actually did, the original source identity, score, queue identity, evidence provenance, or capture time.

The user chooses the desired model before this skill starts; the skill cannot switch or attest models by itself. The closeout receipt must always use `model: unknown`. Do not read, set, or override an environment variable to manufacture model provenance, and never claim a model upgrade that the receipt cannot prove.

## Authoritative Target Set

Use the local date unless the user specifies another date:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py pending --date YYYY-MM-DD
```

This replay is authoritative across tasks. It also returns `active_freezes` and per-capture `active_freeze_ids`; reuse those IDs instead of creating a second freeze for the same capture. Do not reconstruct “today's cards” from the current transcript, card dates, mtime, import batches, or memory. Before any formal write, resolve and freeze:

- ledger hash;
- every pending capture event ID and original content hash;
- amendment IDs and effective evidence hash;
- formal ID or source locator and source hash;
- staged source-bundle manifest path/hash, when the episode introduced new temporary attachments;
- score receipt and study date.

Group events by formal ID or source locator. Multiple same-day events for one card produce one formal-card rewrite, while each event's sequence and facts remain visible in history. A real `new_source` receives duplicate review and ID allocation here, not during study.

If the user supplies explicit IDs, treat them as a requested subset of pending targets. An explicit card outside the ledger is `explicit_external_target`; it may receive a bounded audit, but it cannot be called part of the fast-capture closeout or receive a fabricated capture receipt.

No pending events means stop cleanly. Do not expand into historical maintenance.

## Pre-write Freeze

Read `数学一回滚复习系统/schema/quick_intake_events.md`, then complete the read-only identity plan before touching any formal or derived file. Use `apply_patch` to create one compact JSON file under `/private/tmp`, then run:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py freeze \
  --payload-file /private/tmp/math-fast-intake-freeze-YYYY-MM-DD.json \
  --consume-payload-file
```

Do not use a PTY, pipe, heredoc, inline shell JSON, or shell interpolation. The freeze payload uses `schema_version: math-fast-intake-freeze-v1` and contains exactly:

- `study_date`;
- `scope`: `all_pending_for_date` for a normal nightly run or `explicit_subset` for an explicit pending subset;
- every selected `capture_event_id` exactly once;
- a `target_resolutions` array with one item per grouped formal target; every item has `formal_id`, grouped `capture_event_ids`, `identity_mode`, and `source_binding`.

Use `existing_formal` only when the capture identity and current pre-write card hash agree. A normal `new_source` already contains a daytime-staged repository manifest. Use that exact `source_bundle.manifest_path` as `source_binding.artifact_path`, and use its manifest hash for both `artifact_hash` and `resolved_source_hash`; never restage from the old temporary path. The locator must match the capture. Historical captures without a bundle remain pending until a source-verified amendment binds a real staged manifest. Never freeze a temporary external file, substitute only one child image for the bundle, or map an unresolved source to an arbitrary existing card.

For an `existing_formal` capture with a supplemental source bundle, keep `source_binding: null`; the writer carries the captured bundle into `supplemental_source_bundles` and emits its canonical `fast_intake_source_refs`. Before accepting freeze success, the writer rehashes the manifest and every declared child. Missing, replaced, extra, or symlinked files fail closed.

The returned `freeze_id` is the sole authority for the batch. A failed freeze means no formal write. Reuse the original active freeze after a partial failure; a later unrelated capture stays pending for a later batch and does not invalidate work already started under this freeze. The writer rejects a second freeze containing any capture already owned by an active freeze.

If a core fact must be corrected before formal-card or rollback state changes, obtain explicit user confirmation and run `quick_intake.py abort-freeze --freeze-id ID --reason TEXT`; the append-only abort keeps captures pending, after which `amend` and a new freeze are allowed. Abort fails closed once a target formal card or rollback unit has changed, because automated rollback would destroy pre-applied work. In that case stop for explicit manual reconciliation.

## Optional Luna Preprocessing Consumer

After `freeze` succeeds or an exact active `freeze_id` is resumed, and before reviewing any grouped formal target, query the deterministic read-only consumer once for each frozen capture:

```bash
python3 ~/.codex/study-intake-preprocessor/bin/preprocess_consumer.py consume \
  --subject math \
  --study-date YYYY-MM-DD \
  --capture-id CAPTURE_EVENT_ID \
  --freeze-id FREEZE_ID \
  --effective-evidence-hash EFFECTIVE_EVIDENCE_HASH
```

Take every argument from the frozen inventory; never infer it from chat, the dashboard, or a package filename. The consumer prints one JSON object whose `status` is `ready`, `absent`, `stale`, `failed`, or `disabled`, and binds any ready result with `package_path`, `package_id`, `input_fingerprint`, `validation`, and `adoption_token`; `formal_write_count` must be zero. Business statuses exit zero. A nonzero exit, invalid JSON, nonzero `formal_write_count`, failed validation, or any status other than `ready` means continue immediately with the original Sol review from the canonical ledger, source bundle, and formal card. Do not wait, retry, backfill, invoke Luna, inspect the worker's storage directly, or make package availability a nightly gate.

For `ready`, use only the validated package returned by the consumer as a read-only preparation candidate. Recheck every adopted statement against the frozen evidence and verified source, preserve user facts and hint provenance, and make all semantic decisions and formal writes in this serialized Sol run. The package cannot change target identity, frozen hashes, mastery status, write order, required closeout layers, or receipt model provenance. If the package conflicts with canonical evidence or omits a required fact, reject that part and follow the original workflow.

After Sol finishes the semantic judgment for that capture, record exactly one disposition for the real `adoption_token` returned by its earlier `ready` result. Inspect the consumed package schema before selecting the receipt contract.

For a v1 package, use the legacy compact receipt only:

```bash
python3 ~/.codex/study-intake-preprocessor/bin/preprocess_consumer.py record-adoption \
  --adoption-token ADOPTION_TOKEN \
  --outcome OUTCOME \
  --reason-code OPTIONAL_REASON_CODE
```

For a math v2 package, the receipt must bind Sol's immutable decision and the verified terminal result. A curated result requires `--terminal-outcome curated`, `--formal-id`, and the SHA-256 of the normal formal-write receipt. An already-current result requires `--terminal-outcome already_current`, `--formal-id`, and the SHA-256 of the independent verification artifact. A needs-user or failed result must not carry formal IDs or formal/verification receipt hashes. Pass `--canonical-package-sha256` only when that real artifact exists; pass the item-result ID and SHA-256 only as a pair.

```bash
python3 ~/.codex/study-intake-preprocessor/bin/preprocess_consumer.py record-adoption \
  --adoption-token ADOPTION_TOKEN \
  --outcome OUTCOME \
  --reason-code OPTIONAL_REASON_CODE \
  --sol-decision-sha256 SOL_DECISION_SHA256 \
  --terminal-outcome TERMINAL_OUTCOME \
  --formal-id FORMAL_ID \
  --normal-receipt-sha256 NORMAL_RECEIPT_SHA256
```

Use `direct_adopted` only when the package's relevant semantic proposal is used without material correction. For v2 use `minor_edit_adopted` when Sol makes a narrow wording or boundary correction that preserves the candidate's substance, and `major_edit_adopted` when Sol must materially reconstruct it but it still contributes. Use legacy `modified_adopted` only for v1. Use `rejected` when Sol evaluates the ready package but uses none of its semantic proposal, and `fallback` when a previously ready package cannot be used and the capture returns to the original workflow. Any adopted outcome requires a terminal result of `curated` or `already_current`; it must never be paired with `needs_user` or `failed`. Merely calling `consume`, receiving `ready`, or offering a package to Sol is not adoption. Never invent a decision hash, receipt hash, event ID, outcome, or token; never reuse or transfer them across captures. Non-ready and `disabled` results have no recordable token. If the immediate recording was not durably completed, make one bounded best-effort completion attempt before batch closeout, still bound to the same package and capture. A recording failure affects observability only and must not block or invalidate formal work, rebuilds, or closeout. Keep `reason-code` concise and free of protected question content.

## Authorization And Topology

A normal nightly request authorizes evidence-backed formal-card and verified visual repairs for the frozen targets, one batch rebuild, targeted rollback, derived Wiki synchronization, and a final append-only closeout receipt.

`只读`, `只检查`, or `不要修改` forces report-only mode: read pending events and report defects, but do not freeze, edit cards, rebuild, update rollback/Wiki, or close events.

Keep semantic decisions and every write serialized in one Agent. Independent read-only preparation may be concurrent only when it cannot affect identity, ID allocation, or write order. Do not let parallel workers edit formal files or derived layers.

## Formal Rewrite

For each grouped target, use the capture facts, verified source, current formal card when present, applicable template/rule section, and only the closest identity evidence needed.

- Preserve direct user facts and their event order.
- Keep independently correct steps separate from errors.
- Map the first reusable break to `wrong_point` and, when supported, the single `method_gap`.
- Keep later concept/method/calculation breaks distinct in history, causes, traps, or concise body evidence.
- Preserve assistant-supplied hints as hints, never as independent user mastery.
- Leave unsupported personal claims, same-method recurrence, mastery, or relations pending.
- Merge multiple captures without discarding earlier failures or double-counting one event.
- For a new source, verify the real source, check identity, re-read the current subject maximum immediately before allocation, and allocate one formal ID.

Every changed or created formal card must add exactly the frozen canonical `fast_intake_refs` tokens emitted by the schema contract. It must also add every new canonical `fast_intake_source_refs` token returned by freeze, whether the bundle establishes a new-source identity or is a supplemental attachment to an existing formal card. These are flat frontmatter lists, not prose, comments, or coincidental body substrings. Do not pre-seed them before freeze; a target with a new source reference cannot be reported as `unchanged`, and `new_source_merged` must be an actual `updated` result.

Use the staged child files as the authoritative visual inputs. Copy or project them into the formal visual-asset layout only when the card's final representation requires it, and keep the manifest-backed source immutable. Do not delete the staged bundle after projection.

Patch each formal card once after its evidence plan is stable. Preserve unrelated dirty-worktree changes.

Before the batch rebuild, run the targeted read-only post-write lint once for every changed or created formal card:

```bash
python3 ~/.codex/study-intake-preprocessor/bin/math_formal_lint.py \
  --freeze-id FREEZE_ID \
  --card REPOSITORY_RELATIVE_CARD_PATH
```

Repeat `--card` for every changed or created target in that exact freeze. The linter must recompute the canonical capture and source token sets from `FREEZE_ID`; do not omit `--freeze-id` in nightly closeout. The result must be `status: passed`, `issue_count: 0`, and `formal_write_count: 0`. A card outside the freeze, an extra, missing, duplicate, or syntactically malformed capture/source token, a missing or hash-mismatched source artifact, a missing required frontmatter/body field, tab/control character, malformed LaTeX spacing command, or unbalanced math delimiter is a formal quality-gate failure: fix only the targeted card, rerun the same lint, and do not start wrongnet rebuild or closeout until it passes. This gate validates the final artifact independently and must not be cited as proof that a Luna candidate caused or corrected the defect.

## One Batch Rebuild

After all target formal cards and verified visuals are final, run exactly one successful:

```bash
python3 错题知识网络/scripts/wrongnet.py rebuild
```

Do not rebuild per card. A diagnosed transient/environment failure may receive one scoped retry. Only the successful final run counts; record `batch_receipts.wrongnet.rebuild_count: 1` in the closeout receipt. This count is a workflow declaration, not a cryptographic execution log; the writer proves currentness by re-parsing every target formal card with the canonical wrongnet parser and comparing it with the submitted target projection, then rehashing all nine outputs.

If rebuild still fails, stop before rollback, relationship, Wiki, or closeout. Treat generated files as an untrusted mixed generation and keep every event pending.

## Targeted Rollback

After the successful batch rebuild, perform the repository's targeted wrongnet-to-rollback upsert and verification for each final formal ID when required. Use event evidence to distinguish recurrence from metadata-only repair. Never increment recurrence twice for multiple representations of the same capture.

There is no `batch_receipts.rollback` key. For `wrong_recorded` and `recurrence_recorded`, `capture_results[].durable_record` is the rollback receipt: it must reference a freeze-new processed event in the target unit, the unit's current canonical hash, the final card source version, and for recurrence a unique current-date `正式错题复发` history entry. The changed formal card must also contain the exact canonical capture/evidence reference. Representation and mastery outcomes use the formal-card or closeout binding defined by the schema and must not invent a rollback event.

Any required durable-record failure leaves the affected capture pending and prevents a final batch closeout that includes it.

## Relationship Proposals

Read the local relationship policy. While mode is SHADOW with formal writes disabled, do not edit formal `related` or relationship rationale.

For each target, inspect only existing declarations and at most five serious candidates. Produce `保留提案`, `删除提案`, or `需用户裁决` from independent concrete evidence: the same trigger plus another structural signal, the same first action, the same specific trap, a specific template plus method, or a verified progressive topic chain. Broad chapter, generic cause, text similarity, or generated score alone is insufficient.

Record the layer as `proposal_only` unless policy genuinely allowed and verified a formal write.

## Wiki Closeout

Wiki is a required closeout layer. Only after formal, rebuild, and rollback pass, route the frozen final IDs through:

1. `llm-wiki-ingest` in math single-system incremental mode;
2. `llm-wiki-lint` in targeted/final mode;
3. this skill's `scripts/wiki_parity.py` with `--date ARTIFACT_DATE`, one repeated `--id` per frozen ID, and without `--global` for normal nightly target verification. `ARTIFACT_DATE` is the validated `updated_at` of the current `wrong_questions.json`, not necessarily the capture's study date.

Preserve stable Wiki IDs and update only affected summaries, indices, coverage, matrix, clusters, and log. Do not restore retired destructive full-cluster generators or infer completion from count equality.

For each affected source summary, set `formal_projection_sha256` to the canonical hash of that target's current object in `错题知识网络/生成/wrong_questions.json`. Wiki passes only when local-schema lint, target freshness, target identity, index/coverage/matrix uniqueness, stable cluster references, cluster membership, and this projection hash all pass. A missing safe updater leaves the layer pending; do not create an ad hoc full-library generator. Tutor synchronization is optional and never a closeout gate.

## Final Closeout Receipt

After every required layer passes, use `apply_patch` to create one compact receipt under `/private/tmp`, then run:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py close \
  --receipt-file /private/tmp/math-fast-intake-closeout-YYYY-MM-DD.json \
  --consume-receipt-file
```

Do not use a PTY, pipe, heredoc, or inline shell JSON. Successful or noop closeout consumes the temporary receipt; a failed gate keeps it for repair.

Use `schema_version: math-fast-intake-closeout-v2`, the returned `freeze_id`, and `model: unknown`. The receipt must contain the following exact structured layers:

- `formal_results`: one item per frozen formal target with `formal_id`, `operation` (`created`, `updated`, or `unchanged`), current repository-relative card path, current SHA-256, and real `changed_fields`;
- `capture_results`: one item per capture with the frozen formal mapping, an outcome compatible with the requested action, and a durable record;
- `batch_receipts.wrongnet`: `rebuild_count: 1`, exact hashes for all nine generated artifacts, and the canonical target projection hash for every formal ID. The close writer derives and persists `artifact_date` from the current wrongnet snapshot; it may be later than `study_date` for a delayed retry;
- `batch_receipts.relationships`: a structured per-target SHADOW decision and concrete evidence, or a true `not_applicable` with no proposals;
- `batch_receipts.wiki`: one source-summary path and current hash per formal ID;
- `batch_receipts.visuals`: `passed` with every current visual artifact hash when the formal card has visual references, otherwise `not_applicable` with an empty artifact list.

Map outcomes exactly: `record_wrong` to `wrong_recorded`, `record_recurrence` to `recurrence_recorded`, `update_representation` to `representation_updated` or `representation_already_current`, and `mastery_candidate` to `mastery_confirmed` or `mastery_rejected`. Wrong and recurrence outcomes must bind a real processed record in `复习单元.json`, its current canonical unit hash, and the final card source version. Representation and mastery outcomes bind the final formal-card hash or the frozen evidence hash as defined by the schema; never invent a wrong-event ID for a rejected mastery candidate.

The close writer independently reloads the freeze, capture amendments, formal cards, rollback units, wrongnet projections, Wiki summaries and indices, clusters, visual files, source manifests, every manifest child, and artifact hashes. It includes the manifest and every child in `verified_artifacts`, then rehashes them again after `closeout_prepare`. It appends a closeout event only if all checks pass; it never edits capture or freeze events.

Rerunning the identical receipt returns `noop`. A capture already closed by a different receipt fails closed.

## Partial Failure

Do not append closeout for any event whose required layers are incomplete. A missing or changed source-manifest child is a source-integrity failure, not a visual warning; invalidate any prepare, keep the capture pending, and recover the exact source before retrying. Its pending state is intentional and retryable.

On a later retry:

1. replay the ledger again;
2. reuse and verify the original `freeze_id` rather than freezing already-written formal work again;
3. reuse completed formal or derived work only after rehashing it;
4. perform only the missing or invalid layer without double-counting;
5. create a fresh receipt and append closeout after all gates pass.

A later unrelated capture does not invalidate the frozen subset. If another capture for the same formal card arrived before the frozen card rewrite, the close writer rejects the stale source version. Preserve its original capture and use a source-verified `amend.target_patch.source_hash_before` to rebase only that unfrozen capture to the current unique formal-card hash; then retry the original close and process the later capture separately. This rebase requires `reason.origin: source_verified` and evidence canonically identical to the capture's current effective evidence. It changes provenance binding only, not the user's evidence, and must not count the same attempt twice.

Do not amend a capture still owned by an active freeze. Before any formal write, an explicit safe abort can release it for amendment and regrouping. A later representation-only correction after successful close becomes a new `update_representation` capture. If new evidence overturns source identity, core user facts, or whether the attempt was wrong after formal state already changed, stop for explicit manual reconciliation; do not auto-close contradictory evidence.

Never hide a pending layer behind “夜间收口完成”.

## Completion

Report:

- frozen capture IDs and grouped formal/source targets;
- one concise line per target with actual changed fields and preserved evidence;
- formal, single rebuild, rollback, relationship proposal, Wiki ingest, parity, and lint states separately;
- closed event IDs, remaining pending IDs, `study_date`, derived `artifact_date`, receipt model `unknown`, total duration, and average duration per grouped card.

Use `夜间正式收口完成` only after the deterministic closeout receipt returns `recorded` or `noop`. If anything remains pending, name the exact layer and leave the event retryable.

## Local read-only MCP preparation

Freeze the exact capture set through the canonical ledger first. After the freeze, eligibility is true for cold preparation evidence consisting of the frozen formal IDs, safe formal-card fields, activity evidence, direct relations, and review snapshots. When eligible, the only allowed MCP binding is the math-only server `kaoyan_math_read`, tool `math_read_bundle`, exposed in Codex as `mcp__kaoyan_math_read__math_read_bundle`; use that fully qualified tool for every chunk. Global namespaces `mcp__kaoyan_read__math_read_bundle`, `mcp__kaoyan_read_v2__math_read_bundle`, and `mcp__kaoyan_read_v3__math_read_bundle` are legacy and must not be called. The 408 and English namespaces are cross-subject and must not be called. Freeze, amend, writer, rebuild, Wiki, closeout, receipt, visual detail, and canonical transaction commands remain terminal-only.

Every MCP request must use `schema_version=study-read-mcp.v3` and route context for caller `kaoyan-math-nightly-qa`, Skill version `2.1.0`, the installed plugin version, a unique route request ID, one canonical evidence-scope hash, ordered chunk metadata, and `consumed_duplicate_read_count=0`. Deterministically split scopes larger than 24 formal IDs. All chunks must bind one generation and authority fingerprint; any failed, missing, or mismatched chunk invalidates the entire preparation route.

Before the first eligible call, check the callable task snapshot for the exact required tool `mcp__kaoyan_math_read__math_read_bundle`. If that exact tool is absent, legacy or cross-subject namespaces do not satisfy the requirement: call no MCP tool, record `read_route=terminal_fallback`, `required_mcp_tool=mcp__kaoyan_math_read__math_read_bundle`, `fallback_reason=tool_snapshot_missing`, `chunk_index=0`, and `chunk_count=0`, then immediately run the whole canonical preparation path without probing, retrying, or restarting Codex. On any other failure, drift, inconsistency, timeout, or limit from the required math tool, discard all MCP chunks and rebind the complete preparation scope to one fresh canonical terminal generation. Never combine MCP and fallback evidence.

After MCP success, consumed terminal duplicate reads for the covered preparation evidence must be zero. Before every formal write, the deterministic writer must independently reopen and validate canonical sources; this writer-side revalidation is a separate authority boundary and is not counted as a duplicate preparation read. MCP never performs or authorizes a write and must report `formal_write_count=0` and `model_call_count=0`.
