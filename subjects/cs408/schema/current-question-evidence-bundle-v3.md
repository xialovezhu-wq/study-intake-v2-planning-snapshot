Current Question Evidence Bundle v3

Purpose

`current-question-evidence-bundle-v3` is the current private producer contract. The
answer-safe manifest remains `current-question-evidence-manifest-v1`.

Question mode

The exact `question_mode` is either `dialogue_only` or `image_question`.
`dialogue_only` has no image attachment. `image_question` requires at least one
`question_image` and at least one `solution_image`. A missing required role fails
before attachment or bundle publication. New production cannot issue an
`evidence_pending` receipt, Capture ID, handoff, or public Capture ledger row.
Historical pending objects remain read-only compatible and are not migrated.

Completeness gate

Before any Capture-eligible bundle is published, it must contain a nonempty original
question surface; exactly four options labelled A, B, C, and D in that order with
nonempty distinct text; a nonempty learner answer and choice that agree; a learner
answer event that binds the same choice; and either a nonempty
`standard_explanation` or `grader_basis`. Any missing item fails closed as
`capture_pending_recovery`, with `capture_id=null`, `background_handoff=null`, and
`advance_allowed=false`.

Ordered attachment rows

There are at most eight rows. A ninth row fails closed. Rows preserve input order and
use contiguous `ordinal` values beginning at 1. Each row contains exactly:

```text
ordinal
role
original_bytes_ref
sha256
declared_mime_type
detected_mime_type
detected_format
byte_count
label
```

`role` is `question_image` or `solution_image`. `original_bytes_ref` is
`current-question-attachment://sha256/<sha256>` and resolves to the exact original
private bytes. Declared and detected types must agree. Only PNG, JPEG, and WebP magic
is accepted, with canonical MIME values `image/png`, `image/jpeg`, and `image/webp`.

Trace v2

The mandatory private trace schema is `current-question-interaction-trace-v2` and
contains exactly `schema`, `events`, `original_event_count`, `included_event_count`,
`omitted_event_count`, `omitted_ranges`, `truncation_reason`, and
`full_trace_sha256`.

At most 24 events are included. Included event ordinals retain their original
strictly increasing values. Every omitted range contains exactly `start_ordinal` and
`end_ordinal`. No omission requires an empty range list and
`truncation_reason=null`; any omission requires a nonempty reason. The full trace
digest is always a lowercase 64-character SHA-256.

Trace supplements concatenate existing episode events first and current-turn events
second. Text equality is not a deduplication key. Repeated original words from
different turns remain separate events in their real order.

Bounded attachment input

The sole CLI input is `--attachments-json`. The UTF-8 JSON object is at most 64 KiB
and contains exactly `question_mode` and `attachments`. It defaults to
`{"question_mode":"dialogue_only","attachments":[]}`. There are at most eight
attachments, and each contains exactly `path`, `sha256`, `mime_type`, `role`, and
`label`. Each path must name a regular non-symbolic-link file of at most 16 MiB.
SHA-256, declared MIME, and PNG/JPEG/WebP bytes must agree. The path is invocation-only
private input and is absent from operation identity, bundles, manifests, Captures, and
output. Operation identity binds only ordered role, label, MIME, SHA-256, and byte
count. Corrections cannot resubmit attachments and reuse the first bundle.

Publication boundary

The new producer returns only `evidence_status=ready`. Incomplete production input
uses the technical recovery result described above and does not publish a bundle or
Capture. No model or formal writer is called by this producer.
