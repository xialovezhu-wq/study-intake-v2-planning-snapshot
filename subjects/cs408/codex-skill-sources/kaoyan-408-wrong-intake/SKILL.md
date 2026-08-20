---
name: kaoyan-408-wrong-intake
description: "408 当前题的答案安全快速捕获与正式事务边界。预处理题包中的选项作答只走 answer-current-and-next；独立正确生成私有 observation，失败题生成 bundle v3/capture，wrong 类在同一题纠正后补齐 trace supplement；Luna v2 只提候选，显式日期后由 Sol 核验。"
---

# Kaoyan 408 Wrong Intake

## Goal

Preserve the first answer and the bounded teaching exchange without slowing study.
Knowledge classification, history matching, relations, Luna execution, and formal
writes stay outside the learner turn.

## Exact entry

For every first or correction A-D reply call exactly once:

```text
python3 scripts/managed_408_current_turn.py --repo <repo> --private-root <root> answer-current-and-next --display-receipt-locator <current-question-turn://sha256/...> --choice <A|B|C|D> --confidence <high|medium|low> --prompt-level <none|L1|L2|L3|L4|L5> --trace-json '<JSON list>' --attachments-json '<JSON object>'
```

The external schema is `managed-408-answer-current-and-next-v1`. Do not separately
call `date`, `prepare-current-turn`, `current-turn`, `continue-current`, `next-item`,
the retired direct router, or a capture writer. Do not reconstruct context JSON from
tool output.

Keep the original display receipt throughout same-question correction. Pass only this
turn's observed events in `--trace-json`; every event has exactly `role`, `kind`, and
`text`. Roles are `learner|assistant`; kinds are
`utterance|first_action|reasoning|hint|correction|restatement|answer`. The runtime
adds an exact canonical choice unless one already matches. A canonical choice event
that conflicts with `--choice` fails closed; free-form answer speech is preserved and
receives a separate canonical choice binding. The runtime owns cumulative trace state
and appends prior events before current-turn events without text deduplication.
Identical utterances from different turns remain separate ordered evidence events.

`--attachments-json` is the only attachment input. Its default is
`{"question_mode":"dialogue_only","attachments":[]}`. The top level contains only
`question_mode` and `attachments`; each attachment contains only `path`, `sha256`,
`mime_type`, `role`, and `label`. The private path is read for this invocation only
and never enters the operation, bundle, Capture, or output. A correction must not
resubmit attachments and reuses the first bundle's frozen attachment objects.
`dialogue_only` has no attachments. `image_question` requires both `question_image`
and `solution_image`; missing either fails before Capture creation.

## First-answer split

- High-confidence, unprompted `independent_correct`: one private
  `current-question-evidence-bundle-v3` bound to exactly one
  `current-question-study-observation-v1` at `awaiting_background_analysis`. It is
  one central-consumable Luna candidate, creates zero failure Captures, and stays
  outside the formal wrong-question batch.
- `fragile_correct`: no observation; one `current-question-evidence-bundle-v3` and
  one answer-safe `awaiting_daily_curation` capture. With complete evidence its
  background handoff becomes `ready` with `completion_kind=first_turn_complete`,
  then receipt-gated advancement. An image-role gap fails before Capture creation.
- `wrong|partial|uncertain`: no observation; the same bundle-v3/capture pair under
  `current_question_failure_standing_policy_v1`, but the result is
  `feedback_ready_continue_current`; handoff stays `teaching_pending`, the first-turn
  receipt has `advance_allowed=false`, and the
  same display receipt is retained for the next correction answer.

All branches have `luna_call_count=0` and `formal_write_count=0`. An observation is
positive study evidence, not mastery. A capture is a durable fact handoff, not a
formal node, classification, recurring-error conclusion, relation, or formal intake.
A newly created post-attestation failure Capture must carry
`producer_binding_status=attested` and a non-null content-addressed
`producer_binding_attestation_sha256`. `historical_pre_attestation` applies only to
immutable captures before the declared high watermark and must never be backfilled.
The sidecar binds this foreground Skill, the Producer closure and Capture contract,
but remains release-neutral and contains no release, activation, Dispatcher or MCP
authority identity.
Every completed item produces exactly one background candidate: its study observation
or its one Capture with the final ready handoff. Corrections only append the original
trace supplement and cannot create another candidate, bundle, or Capture.

## Same-question correction

Use the same command and display receipt for each correction answer.

- Still wrong: return the prepared correction, retain the item, and merge this turn's
  private trace. Do not repeat the immutable first outcome, bundle, or capture.
- Correct after teaching: publish one `teaching-resolution-attestation-v1`, one
  `current-question-trace-supplement-v1` bound to the original capture/evidence
  manifest, and one session resolution `relearn_required`. It explicitly records
  `mastery_effect=none`, `retention_effect=none`, and
  `independent_repair=false`. Complete evidence then atomically promotes the handoff
  to `ready` with `completion_kind=teaching_resolved`. Navigation stays receipt-gated.

This resolution says only that the immediate correction was completed. It never
rewrites the original failure into independent correct.

## Trace and privacy

The private `current-question-interaction-trace-v2` is bounded to 24 included events,
2048 UTF-8 bytes per event, and 32 KiB canonical total. It records original, included,
and omitted counts, exact omitted ordinal ranges, a truncation reason, and the
full-trace SHA-256. Included events keep their original ordinals. Any omission is an
explicit evidence gap.

Bundle v3 declares `question_mode=dialogue_only|image_question`. Image attachments
are ordered and bind ordinal, role, an original-bytes content reference, byte count,
SHA-256, declared/detected MIME, and detected PNG/JPEG/WebP magic. Roles are
`question_image|solution_image`; an image question requires at least one of each. At
most eight are accepted and a ninth fails closed. If a required image role is
missing before the operation is established, the external result has
`status=blocked` and
`reason_code=image_question_requires_question_and_solution_images`. It creates no
Capture, background handoff, navigation advancement, model enqueue, or formal write.

Capture-eligible evidence also requires a nonempty question surface, exactly four
nonempty distinct A-D options, the real learner answer bound to the CLI choice and a
learner answer event, plus a nonempty standard explanation or grader basis. Any
missing item after the operation is established uses the applicable
`capture_pending_recovery` path and never creates a pending quality attempt.

The first bundle contains the first-turn trace. On teaching resolution, the private
trace supplement contains the bounded cumulative exchange and its hash. Public
capture rows contain neither trace text nor the complete stem, answer, options, full
learner response, feedback, grader, image, handwriting, or private path.

## Hot-path prohibitions and recovery

The learner turn reads no personalization, MEMORY, history, formal nodes, knowledge
indexes, relations, old questions, another pack item, Luna, Dashboard, model, Provider,
MCP, Sol, worker/report state, port 8767, daily-curation state, reference books, or
source code. It runs no whole-ledger scan, audit, reconcile, generation, status, or
schema discovery.

Show only authorized feedback and a returned next surface when
`next_item_published=true`. An idempotent replay creates zero new learner writes. Receipt
and `recover-current-capture` recovery reuse original identities and only add the
missing type-preserving layer. Independent-correct evidence recovery creates an
observation and never a failure Capture; capture-eligible recovery creates only its missing
bundle/capture. Successful recovery closes the original answer operation and may
return its receipt-gated successor. New incomplete image evidence has no Capture or
handoff to promote. Old advancing receipts are rejected unless their
item is the current navigation frontier. Recovery never duplicates an outcome,
observation, capture, resolution, supplement, or successor.

## Deterministic background candidates
Outside the learner turn, deterministic preprocessing must pin a complete-matching,
answer-safe knowledge/history snapshot to the evidence manifest and exact source
hashes. It scans every taxonomy row, formal wrong-question row, formal relation row,
and review record, then records a content-addressed coverage manifest with total,
scanned, matched, included, excluded, truncation, and deterministic partition counts.
Configured candidate limits are partition sizes, never silent top-N caps. Every exact
or verifiable knowledge match retains all available ledger-backed error dates; file
mtime and chat recollection are not evidence. Membership proves only candidate
availability.
The snapshot separates exposed-error knowledge, all involved knowledge, explicit
upper/lower candidates, adjacent-or-confusable candidates, historical wrong questions,
existing formal edges, and proposal-only semantic relations. Parent/child status is
`hierarchy_unavailable` unless the controlled source explicitly proves it. The input
identity binds the capture, latest resolved trace and handoff, knowledge source hashes,
coverage manifest, processing contract, and release. A source or trace change creates
a new fingerprint and supersedes the old result.
If runtime has not supplied a verified snapshot schema, locator, hash, and binding,
report it missing or limited; do not invent them. The chat never repairs it by
searching the repository.

## Luna proposal boundary
The private `current-question-background-handoff-v1` object and
`current-question-background-handoff-binding-v1` use
`current-question-background-handoff://sha256/<object_sha256>`. Consumers resolve the
gate only by capture ID. Missing, unsafe, drifted, unknown, `teaching_pending`, or
`evidence_pending` state is defer/fail closed; only a fully verified `ready` object is
eligible.
The chat never starts, waits for, polls, retries, or inspects Luna, its worker,
consumer, dashboard, report, or port 8767. Awaiting states do not prove Luna ran.
`ready` also proves input completion only, not Luna execution. For a
wrong/partial/uncertain capture, semantic analysis remains gated until the
capture-bound `resolved_trace` supplement exists.
The producer-side observation, bundle, and Capture are release-neutral and contain no
`release`, `activation`, Dispatcher authority, or MCP authority. Only the central
Dispatcher injects those fields after consuming the neutral candidate.
The artifact remains `study-intake-luna-analysis-v2`. Controlled execution is fixed
to `model=gpt-5.6-luna`, `reasoning_effort=max`, and the only consumable pipeline
status is `two_pass_ready`. It may propose learning breaks, exposed and involved
knowledge, complete matched prior-error timelines, existing-edge reuse,
new-wrong-question relations, knowledge-hierarchy candidates, and contrast/confusion
candidates. Every proposal needs stable input-bound identity, endpoints, relation
type and strength, source refs, historical error times when available, confidence,
counterevidence/boundary, and a Sol verification action. Both analysis and independent
critical review must pass against the same input fingerprint. Luna remains
`proposal_only`, with `formal_write_count=0`; it cannot change learner facts, captures,
formal identity, taxonomy, relations, mastery, schedule, or state. A v1 report remains
auditable as `legacy_insufficient_for_knowledge_network` and cannot become the current
v2 result.

## Explicit-date Sol curation
Only an explicit user-specified `Asia/Shanghai` date freezes failure captures. Sol
then runs the explicit-date Sol batch; background Luna never authorizes it. Sol
reopens the raw capture, bundle-v3 trace, resolved supplement when required, verified
pinned snapshot, and at most one consumable Luna v2 report. It independently reopens
every proposed source and records adopt/modify/reject before one serial batch-size-1
formal transaction. A correct-only observation remains outside the wrong batch.

An immutable user/source/date/answer/provenance conflict or unresolved identity is
`needs_user` with apply count zero. A successful apply with failed closeout resumes
only from its original receipt and is never reapplied.

managed_fast_intake_route_v11=entry:managed-408-answer-current-and-next-v1-only;correct:current-question-evidence-bundle-v3-plus-study-observation-v1-central-candidate;failure:current-question-evidence-bundle-v3-plus-one-capture;candidate:exactly-one-per-completed-item;correction:same-display-trace-supplement-no-duplicate-candidate;recovery:type-preserving-and-operation-closing;navigation:frontier-bound;handoff:ready-only-after-completion;neutral:no-release-activation-authority;luna:no-hotpath-wait-or-query;formal:explicit-date-sol-adopt-modify-reject
