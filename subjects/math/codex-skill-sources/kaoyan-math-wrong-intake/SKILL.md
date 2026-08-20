---
name: kaoyan-math-wrong-intake
description: "Fast, source-backed capture for exactly one math question during active study. Use when the user says 入库, 更新这道题, 记录复发, 标记已掌握, or asks to save the current GS/LA/PR/new-source math mistake. Every fresh capture, including recurrence on an existing formal card, first stages a real question image and exactly one UTF-8 solution_text file as an immutable hashed source bundle, then appends one release-neutral v2 pending-nightly evidence event. Do not use a formal-card answer or a solution image as solution_text. Do not rewrite the formal card, rebuild wrongnet, update rollback, inspect relations, or touch Wiki in the daytime path. Use the synchronous closeout-v2 escape route only when the user explicitly requests 立即完整正式入库 or 现在重建并回滚. Route 夜间集中优化 to kaoyan-math-nightly-qa. Never ingest model-authored questions."
---

# Kaoyan Math Wrong Intake

## Outcome

Capture one real current-question episode quickly and durably, return an idempotent receipt, and release the study conversation. The normal completion state is `pending_nightly`; it is a successful evidence capture, not a completed formal-card rewrite.

The two-stage design preserves quality by separating roles:

- daytime capture stores immutable learning facts and provenance; every fresh capture first freezes its real question image and independent solution text into an immutable hashed source bundle;
- nightly QA improves formal representation and derived layers without rewriting those facts.

## Route Before Acting

Choose exactly one route:

1. `fast_capture` is the default for explicit 入库、更新、记录复发、掌握候选 during active study.
2. `nightly_batch` applies to 夜间质检、睡前集中优化、优化今天所有入库题; route to `kaoyan-math-nightly-qa`.
3. `immediate_full_closeout` applies only when the user explicitly requests synchronous formal-card rewrite, rebuild, and rollback now. It still uses capture, freeze-v1, and closeout-v2; read `references/immediate-full-closeout.md` only for that route.
4. A result report without an explicit save/update request records only the canonical warmup score when applicable; it does not authorize a capture or formal write.

`标记已掌握` in fast mode creates a `mastery_candidate`. It does not directly rewrite the formal mastery fact.

Reject model-authored variants, transfer questions, and generated exercises. A source-verifiable user-studied question is required.

## Fast-path Budget

The user is waiting to continue the next question. Every fresh capture uses the same bounded path of up to 5 calls: create the stage payload, run `stage-source`, create the capture payload, run `record`, and at most one deterministic retry. Never exceed 5.

Do not use a plan, subagents, memory lookup, full-task transcript scan, relation lookup, nearby-card comparison, full-library search, or ad hoc helper script. Do not read `quick_intake.py` or its schema during capture; the fixed contract below is sufficient.

Use only the current-question episode already in context. If compaction or an actual identity conflict makes the episode unsafe, save a source-safe `new_source` capture only when a stable locator and staged source bundle exist. The writer rejects a new source without that bundle; do not create a `needs_user` capture and do not claim success from a temporary path. Otherwise ask one minimal question. Do not spend minutes reconstructing optional detail.

Fast capture may write only:

- the exact canonical warmup score, if this is a scoreable delivered item and the score is not already recorded;
- one deterministic immutable source bundle under `数学一回滚复习系统/快速入库来源/` for every fresh capture;
- `数学一回滚复习系统/快速入库事件.jsonl` through the bundled deterministic writer.

It must not edit a formal card, `复习单元.json` except through canonical scoring, wrongnet output, Wiki, relations, visual mappings, or any other question.

Performance objective: normal end-to-end capture within 30 seconds; acceptance gate: measured P95 no more than 60 seconds. Until measured evidence exists, describe this as a target rather than a proven guarantee.

## Identity And Score

For a delivered warmup item, use the actual `delivered_card_id`, never the private anchor. Recover only the exact `queue_id`, `queue_item_id`, and `selection_mode` already attached to this item.

For scoreable modes, run first when needed:

```bash
python3 数学一回滚复习系统/scripts/scheduler.py score-warmup QUEUE_ID QUEUE_ITEM_ID SCORE --date YYYY-MM-DD
```

Capture the returned stable `SCORE-...` event ID. Repeating the same score is allowed only as the scheduler's idempotent noop. For `supplemental_training`, do not call `score-warmup`; capture the session evidence without a long-term score reference.

For a named formal card outside warmup, use its unique GS/LA/PR ID and reverify its current source version. Select a real repository-backed question image or the current episode's real question image, and stage it together with the episode's independent solution text; do not use the formal-card answer as a fallback. For a real new question without a formal ID, use `target.kind: new_source`, a stable source locator, the staged bundle's manifest hash, and no invented ID. Nightly QA owns duplicate checking and ID allocation.

## Evidence Capsule

Capture the smallest capsule that preserves the whole current question:

- one or more direct user facts;
- independently correct steps, if any;
- the first knowledge, concept, condition, or method break;
- later distinct breaks, kept separate from the first;
- hints needed and self-corrections;
- the 0–5 mastery score and its basis;
- unresolved points.

Each item carries one provenance token:

- `user_observed` or `user_confirmed` for real user evidence;
- `source_verified` for queue, score, question, or file facts;
- `assistant_inferred` for a model interpretation;
- `assistant_explained` for a hint or explanation supplied by the assistant;
- `unresolved` for uncertainty.

Never relabel assistant teaching as the user's independent behavior. The capsule requires at least one direct user fact. Preserve correct work as correct work; do not turn it into an error merely to fill the card.

## Source Durability Gate

Before building any fresh capture, assemble one source-stage payload. This applies equally to a warmup item, an existing formal-card recurrence, an update, and a new source.

1. Include at least one real `question` image. Its extension and validated content must be PNG, JPEG, or WebP. An existing formal card may supply a verified repository-backed question image, but the current capture must still stage it into its own bundle.
2. Create exactly one UTF-8 `.txt` or `.md` file whose content is the verified solution text for this episode. Do not obtain it by falling back to the formal-card answer. Normalize only the artifact bytes to LF, trim leading and trailing whitespace, and retain exactly one final newline; keep the user's original `episode_evidence.solution_text` byte semantics in the Capture.
3. A solution image may be included as optional supporting evidence, but it never replaces the path-backed `solution_text` artifact.
4. Sources under `/var`, `/private/tmp`, `/tmp`, a clipboard cache, or another external location are valid only as temporary stage inputs. Never put those absolute paths into a manifest, Capture field, or model-readable text.

Use `apply_patch` to create a compact stage payload under `/private/tmp`:

```json
{
  "schema_version": "math-fast-intake-source-stage-v1",
  "study_date": "YYYY-MM-DD",
  "source_locator": "STABLE SOURCE LOCATOR",
  "artifacts": [
    {"role": "question", "path": "/absolute/path/to/question.png"},
    {"role": "solution_text", "path": "/absolute/path/to/solution.txt"}
  ]
}
```

Allowed roles are `question`, `solution`, `solution_text`, `user_work`, and `reference`. `solution_text` accepts only UTF-8 `.txt` or `.md`; every bundle has exactly one. Stage all currently available attachments for one source in one call; the same date and locator are immutable. Then run:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py stage-source \
  --payload-file /private/tmp/math-fast-source-STABLE_ID.json \
  --consume-payload-file
```

Success is `recorded` or `noop` with `state: source_staged`, a repository-relative `manifest_path`, and its `manifest_hash`. The writer validates file type and magic, rejects symlinks and empty or oversized files, normalizes `solution_text` deterministically, copies atomically, records each child's SHA-256, and verifies the completed bundle. The manifest and every child path remain repository-relative. Same identity and different bytes fails closed. If staging fails, do not run `record` and do not claim capture success.

If a genuinely new attachment arrives after the bundle was staged, never overwrite or append to that bundle. Give the later attachment a new stable supplemental locator and record it as a separate source-backed representation event; nightly identity review may merge both events into the same formal card.

## Deterministic Record

Use `apply_patch` once to add a compact JSON file at a stable temporary path such as `/private/tmp/math-fast-intake-QUEUE_ITEM_ID.json`. Creating a new file avoids the context-matching failures of editing a long formal card. Do not use a PTY, pipe, `printf`, heredoc, shell interpolation, or inline JSON argument.

Then run exactly one writer command:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py record \
  --payload-file /private/tmp/math-fast-intake-QUEUE_ITEM_ID.json \
  --consume-payload-file
```

On `recorded` or `noop`, the writer removes the temporary payload. On validation or persistence failure, it keeps the same file for one deterministic retry.

Use this exact top-level shape:

```json
{
  "schema_version": "math-fast-intake-capture-v2",
  "attempt_id": "STABLE_ATTEMPT_ID",
  "study_date": "YYYY-MM-DD",
  "target": {
    "kind": "formal_card",
    "formal_id": "GS-000",
    "source_locator": null,
    "source_hash_before": null
  },
  "score_event_id": "SCORE-... OR null",
  "requested_action": "record_recurrence",
  "thread_ref": null,
  "source_bundle": {
    "manifest_path": "数学一回滚复习系统/快速入库来源/YYYY-MM-DD/BUNDLE_ID/manifest.json",
    "manifest_hash": "64-CHAR LOWERCASE SHA-256"
  },
  "episode_evidence": {
    "solution_text": "VERIFIED SOLUTION TEXT OR CURRENT TEACHING SOLUTION",
    "user_answer_text": "USER'S ORIGINAL REASONING OR ANSWER",
    "teaching_turns": [
      {"speaker": "user", "kind": "reasoning", "text": "CURRENT USER REASONING", "origin": "user_observed"},
      {"speaker": "assistant", "kind": "hint", "text": "RELEVANT HINT", "origin": "assistant_explained"}
    ]
  },
  "evidence": {
    "result": "wrong",
    "user_facts": [{"text": "DIRECT FACT", "origin": "user_observed"}],
    "independent_correct_steps": [],
    "first_break": {"kind": "method_trigger", "text": "FIRST BREAK", "origin": "user_confirmed"},
    "later_breaks": [],
    "hints_needed": [],
    "self_corrections": [],
    "mastery_score": 2,
    "mastery_source": "warmup_score",
    "score_basis": {"text": "BASIS", "origin": "source_verified"},
    "unresolved": []
  }
}
```

`source_bundle` is mandatory for every fresh capture. Its manifest must contain at least one real `question` image and exactly one path-backed `solution_text` artifact. For `new_source`, set `target.source_hash_before` to the same manifest hash and keep `target.source_locator` exactly equal to the stage payload's locator. For a formal card, the target hash remains the verified current formal-card hash; `source_bundle` supplies capture evidence and does not replace formal identity.

Every fresh capture uses `math-fast-intake-capture-v2`. `episode_evidence.solution_text` and `episode_evidence.user_answer_text` must both be non-empty. Compare the episode solution and staged solution artifact through the same normalization rule: LF line endings, leading and trailing whitespace removed, and exactly one final newline. A mismatch fails before ledger append; do not rewrite the original Capture text to make it match. Keep only the relevant current-question teaching turns, each with `speaker`, `kind`, `text`, and `origin`. Allowed speakers are `user` and `assistant`; allowed kinds are `reasoning`, `hint`, `correction`, `explanation`, `restatement`, and `answer`. Historical v1 events remain read-only compatible and must not be rewritten, converted, migrated, or rerun.

Recursively reject release, activation, Dispatcher authority, MCP authority, consumption state, and handoff state anywhere in the payload. Also reject local absolute paths in persisted Capture fields or model-readable text. Do not add producer-generated substitutes for missing question, solution, user answer, correction, or provenance evidence.

Allowed requested actions are `record_wrong`, `record_recurrence`, `update_representation`, and `mastery_candidate`. Allowed results are `wrong`, `unstable`, `correct`, and `unresolved`. Break kinds are `knowledge`, `concept`, `condition`, `method_trigger`, `method`, `calculation`, `expression`, `identity`, or `unknown`.

For warmup, set `attempt_id` exactly to the score event's attempt ID. The writer authenticates the actual delivered card, date, score, queue identity, match mode, anchor, and source version from `复习记录.jsonl`; do not copy its large evidence snapshot.

The writer already locks, appends, hashes, rereads, and verifies. Declare success only when the writer's real receipt has `status: recorded`, or `status: noop` from the identical-evidence idempotent route, together with `state: pending_nightly`, `payload_file_consumed: true`, and integer `formal_write_count: 0`. A newly recorded post-attestation Capture must also return `producer_binding_status: attested` plus a non-null content-addressed `producer_binding_attestation_sha256`; `historical_pre_attestation` is valid only for immutable events before the declared high watermark and must never be backfilled. The attestation remains release-neutral and contains no release, activation, Dispatcher authority, or MCP authority. A missing, boolean, string, null, or nonzero `formal_write_count` is not success. The foreground Skill stops after that release-neutral receipt; it does not wait for Luna and does not call a model, Provider, MCP, Sol, or formal writer. Do not add manual jq probes, Git status, file-hash snapshots, or a second verification sequence in the live study path.

Outside the latency-sensitive live path, `quick_intake.py verify --date YYYY-MM-DD` verifies both ledger replay and every staged manifest child for that date.

## Failure Recovery

The event ID is stable for `attempt_id + target identity`.

- Same identity and same evidence returns `noop` without a duplicate.
- Same identity with different evidence fails closed. Before freeze, a later factual correction may use the writer's `amend` event. A formal-card source rebase is narrower: it requires `reason.origin: source_verified` and canonically unchanged evidence. An active freeze must be reused or explicitly and safely aborted by the nightly workflow before amendment; representation-only follow-up must not count the same attempt as another mistake.
- If staging succeeds but capture fails, preserve the returned manifest path/hash and retry the same capture once. The source bundle may safely remain even if capture cannot be completed; never delete or overwrite it during recovery.
- If scoring succeeded but capture failed, preserve the score event ID, attempt ID, target, capsule, and retained temporary payload; retry the same writer command once.
- After one failed retry, do not claim success. Return a compact recovery capsule and let the user continue; the score event remains safe and the capture can be resumed later.
- A pending nightly state is normal, not a failure.

## Completion

Only after the receipt success gate above passes, use one short sentence:

`快速记录成功，已进入今晚的正式优化队列；你可以继续下一题。`

Optionally include the event ID. Do not say the formal card, rollback, wrongnet, relations, or Wiki are complete. Do not append an old-question recommendation or open another question automatically.
