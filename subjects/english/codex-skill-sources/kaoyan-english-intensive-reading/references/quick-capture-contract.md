# Resolved-Sentence Quick Capture

Use this immediately after the current sentence has actually been resolved. The command records immutable daytime learning evidence for asynchronous preprocessing; it does not perform formal curation.

## Preconditions

- Confirm stable `source_id`, article-level `source_hash`, `sentence_id`, exact source sentence and its current `sentence_hash` from the intake handoff. These are two distinct bindings: the article hash goes to `--article-sha256`, while the exact sentence hash goes to `--sentence-sha256`.
- Before constructing either direct flags or `--input-json`, actually read the repository article page, its unique repository-relative POSIX `pipeline_handoff` JSON and the handoff's practice-safe canonical payload. Recompute the article hash from UTF-8 payload bytes after CRLF/CR → LF normalization, per-line trailing-whitespace removal and final-LF removal. The actual hash, article hash, handoff hash and requested hash must all agree; otherwise stop before event creation.
- `source_article` denotes the article page and is persisted only as a safe repository-relative POSIX locator. A repository-contained absolute input may be accepted by the CLI, but no local absolute path, `..` escape, missing file, directory or outside symlink may enter the event.
- Preserve the user's first translation verbatim when present. If absent, omit the optional field and retain explicit absence; never generate a substitute.
- Capture only observed evidence. Do not infer independent mastery from a guided correction, model explanation or self-report.
- Do not run this after an intermediate diagnostic question while the sentence is unresolved.

## Command

Run the project CLI rather than editing a projection or formal file by hand:

```text
python3 /Users/xiazhibin/Documents/kaoyan-english/scripts/english_learning_pipeline.py capture --repo-root /Users/xiazhibin/Documents/kaoyan-english --state-dir /Users/xiazhibin/Documents/kaoyan-english/intake --idempotency-key <stable-turn-key> --source-id <source_id> --source-article <repository-relative-article-locator> --article-sha256 <article-source-hash-64-hex-without-prefix> --sentence-id <sentence_id> --source-sentence <exact-source-sentence> --sentence-sha256 <sentence-hash-64-hex-without-prefix> [--source-kind <article-or-question-or-option-or-explanation-or-user_provided>] [--first-translation <verbatim-first-translation>] [--user-evidence <verbatim-user-evidence>] [--evidence-state <observed-state>] [--evidence-kind <observed-kind>] [--translation <resolved-translation>] [--explanation <minimal-explanation>] [--candidate-json <json>] [--supersedes <event_id> --correction-reason <reason>] [--quick-flush]
```

Direct flags and `--input-json` must enter the same source-object validator after request construction. Use a stable turn or message identity for `--idempotency-key`. Retrying the same resolved sentence must replay the existing result; do not generate a fresh random key on retry. Strip the display prefix `sha256:` from both hashes before passing their 64 lowercase hexadecimal values. Do not supply the compatibility alias `--article-id` as a second identity. Use `--supersedes` only with the exact current effective prior `event_id` for an evidence-backed correction; the new event records that immutable link as `supersedes_event_id`, while the old event remains byte-stable. Never use a capture ID and never erase or overwrite history.

Use `--quick-flush` only when the user's current message explicitly says `快速入库`. It publishes a signed, content-addressed intent bound to this exact event and receipt, so the background adapter may form a one-event microbatch immediately. It does not fake `article_completed`, does not change the ordinary 5-capture/180-second policy and does not authorize formal curation. Without that explicit phrase, omit the flag.

`--candidate-json` contains only source-backed proposals and exact evidence enums. Guided understanding is not independent mastery. A mastery proposal requires explicit `independent_correct_use` evidence and remains only a proposal for nightly review.

The CLI must also capture a resolved sentence when there are zero candidates and no observed unknown point. Omit `--candidate-json`, `--first-translation` or `--user-evidence` when their facts are absent; do not manufacture placeholder evidence. The resulting zero-candidate event still needs a real receipt and `formal_write_count=0`.

The command atomically appends an immutable `english_capture_event_v2` under `/Users/xiazhibin/Documents/kaoyan-english/intake/events` and persists an `english_capture_receipt_v2`. New event files are exactly the event's canonical JSON bytes with no final newline, so physical file SHA-256, event object SHA-256 and receipt `event_sha256` must be identical. Do not add release, activation, Dispatcher authority, MCP authority or consumption state to the Capture.

The foreground Skill must not start, invoke or wait for Luna, Provider, MCP or Sol. The background worker may discover the event later, outside this Skill turn. Quick capture never performs a formal English write.

## Receipt gate

Parse the command's JSON output and validate it against `schema/english_pipeline/capture-receipt-v2.schema.json`. An initially persisted receipt may use `projection_status=pending` with `view_path`, `view_sha256` and `projection_error` all null while projection is in progress. The CLI's final returned and persisted receipt must validate with `projection_status=rendered` or `projection_status=failed`; `pending` is not a final CLI success.

Read the immutable event named by `event_path`, require `schema_version=english_capture_event_v2`, verify its physical file SHA-256 equals both the canonical event object SHA-256 and receipt `event_sha256`, then verify that `article.source_article`, `article.source_id`, `article.source_hash`, `source.sentence_id` and `source.sentence_hash` equal the exact validated handoff values. Also require `request_sha256` to correspond to this invocation and both `formal_write_count=0` and `formal_writeback=none`.

Treat `created` or `idempotent_noop` with `projection_status=rendered` as a complete foreground success. Treat `capture_saved_but_projection_failed` with a valid event and receipt as a durable capture plus a failed human-readable projection: report the partial projection failure accurately, retain the same idempotency key for any safe retry, and never call the capture unsaved. Only a missing or invalid immutable event means the capture itself is not proven.

Report:

```text
快速入库回执

receipt_id：<returned value>
capture_id：<returned value>
status：created / idempotent_noop / capture_saved_but_projection_failed
event_sha256：<returned value>
projection_status：rendered / failed
view_path：<returned value or null>
view_sha256：<returned value or null>
formal_write_count：0
formal_writeback：none
quick_flush：created / idempotent_noop / omitted
```

If the command returns no valid durable event receipt, report `快速入库失败` with the safe error code. Do not fabricate a receipt, directly edit the article candidate area, or fall back to bank, mastered, SP or review writes.
