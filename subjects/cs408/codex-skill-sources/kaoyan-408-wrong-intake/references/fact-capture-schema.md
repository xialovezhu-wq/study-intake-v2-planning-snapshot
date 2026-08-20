# Current-question fast capture contract

This reference describes the answer-safe capture emitted by the bounded current turn.
It is not a formal-node package.

## Eligibility

Use `current_question_failure_standing_policy_v1`:

- no capture for high-confidence, unprompted, independently correct reasoning with no
  verified break;
- capture once for correct with medium/low confidence, prompt dependency, or a
  verified break;
- capture once for wrong, partial, blank, or uncertain.

An eligible capture has `formalization_authorized=true` only for admission to a later
explicit-date batch. It always has `formal_write_count=0` and ends at
`awaiting_daily_curation`.

## Public context

The `current-question-context-v1` contains only:

- schema, source, request/session/item/source IDs, and idempotency key;
- explicit event time, local study date, and `Asia/Shanghai` timezone;
- source stability and source-binding SHA-256;
- opaque grader capsule ID;
- current answer-safe public surface or current attachment hashes;
- current learner answer/confidence/first action/reasoning;
- controlled answer-safe assessment and provenance;
- `formal_write_count=0`.

Private keys, correct options, graders, standard solutions, decisive reasons, and
feedback maps are rejected recursively.

## Private evidence bundle

Publish one `current-question-evidence-bundle-v3` in a user-owned private
content-addressed root outside Git and the formal repository. It contains:

- full current question or attachment bytes;
- private answer basis, grader, or user-provided standard explanation;
- actual learner response and reasoning;
- the already frozen assistant feedback;
- context/source/date/provenance bindings;
- object and ordered attachment hashes;
- explicit missing fields.

The exact `question_mode` is `dialogue_only` or `image_question`. Dialogue-only
evidence has no image rows. An image question requires at least one
`question_image` and at least one `solution_image`. Its one-to-eight rows preserve
the original order and bind ordinal, role, original-bytes content reference,
byte count, SHA-256, declared MIME, detected MIME, and detected PNG/JPEG/WebP magic.
A ninth row, role gap, corrupt file, hash/size drift, or declared/detected mismatch
fails closed; a missing required image role is `evidence_pending` and has zero model
enqueue.

The mandatory private `current-question-interaction-trace-v2` keeps at most 24
events and 32 KiB canonical JSON, with at most 2048 UTF-8 bytes per event. It always
records the original, included, and omitted counts, exact omitted ordinal ranges,
truncation reason, and the full-trace SHA-256. Omitted content is an explicit gap and
is never reconstructed.

Directories use mode 0700 and objects mode 0600. Reject symlinks, traversal,
ownership drift, oversized objects, attachment hash mismatch, or unknown fields.

## Capture payload

The capture ledger may store only:

- schema, capture ID, current standing-policy intent, date/timezone, and status;
- answer-safe current source/context/item identifiers;
- private manifest locator, manifest SHA-256, and bundle object SHA-256;
- directly supported answer-safe source and learner facts with provenance;
- answer-safe first-break label when supported;
- explicit gaps, idempotency key, and receipt hashes;
- `formalization_authorized=true` and `formal_write_count=0`.

It must not store a complete stem, answer, options, full response, full reasoning,
frozen feedback body, explanation, attachment bytes, image, handwriting, private local
path, `/tmp`, `/var/folders`, or `file://` locator.

## Transaction order

The current turn freezes feedback first, publishes private evidence when eligible,
commits exact first-answer/session receipts, and then writes at most one capture. A
capture must never influence the already frozen teaching response.

An unstable source returns `capture_unavailable_unstable_evidence` and cannot claim
success. It does not prevent an honest teaching response.

## Recovery

First/session failure uses `recover-current-receipts`, hides answer-bearing feedback,
and does not advance.

Evidence/observation/capture-only failure keeps feedback visible and position
unchanged, and uses `recover-current-capture`. Recovery verifies the original
context/capsule/recovery object and reuses the original identity. It is
type-preserving: independent correct may add only the missing observation and never a
capture; capture-eligible results may add only the missing bundle/capture. Successful
recovery closes the original answer operation and never appends a second outcome.

## Background consumer

Only a durable unfrozen `awaiting_daily_curation` capture with a verified `ready`
background handoff and bundle-v3 evidence status is eligible for background Luna
analysis. `teaching_pending` and `evidence_pending` are not enqueueable. The chat
does not invoke or inspect Luna. Luna output remains private and does not mutate this
payload.

Every real analysis and critical-review stage is fixed to `gpt-5.6-luna` with
`reasoning_effort=max`. Only `two_pass_ready` is consumable. Single-stage output is
a recovery checkpoint only, never formal-consumption evidence.

The later explicit-date Sol workflow reopens this raw payload and private bundle. It
never treats Luna, Sol sidecars, workbench status, or adoption receipts as a fact or
formal completion source.

current_question_fast_capture_v3=policy:failure-standing;public:answer-safe;private:bundle-v3-trace-v2-content-addressed;images:ordered-role-and-magic-bound-one-to-eight;pending:zero-model;terminal:awaiting-daily-curation;model:gpt-5.6-luna-max-two-pass-only;formal-write-count:0;recovery:type-preserving-operation-closing
