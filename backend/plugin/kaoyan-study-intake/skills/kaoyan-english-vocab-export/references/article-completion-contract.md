# Article Completion and Foreground Export

Use this when the user reaches the end of an article or explicitly requests the unified A/B/C export. Completion is a durable foreground event, not a formal write and not a Luna result.

## Command

Run:

```text
python3 /Users/xiazhibin/Documents/kaoyan-english/scripts/english_learning_pipeline.py complete-article --state-dir /Users/xiazhibin/Documents/kaoyan-english/intake --source-id <source_id> --idempotency-key <stable-completion-key> [--date <YYYY-MM-DD>]
```

Use the same idempotency key when retrying the same article-completion request. Do not create a fresh key merely because the command is retried.

The command freezes the effective capture IDs for that article and immediately writes an output-only A/B/C JSON plus Markdown snapshot under `/Users/xiazhibin/Documents/kaoyan-english/intake/views/YYYY-MM-DD/` unless `--output-dir` is explicitly supplied. It appends `article_completed` for the background worker, but it neither runs Luna nor writes formal data.

## Receipt and output gate

Parse the JSON response. Require `schema_version=english_article_completion_receipt_v1`; `capture_status` must be `created` or `idempotent_noop`; both `source_id` and the compatibility `article_id` must equal the intended canonical `source_id`; and the receipt must report both `formal_write_count=0` and `formal_writeback=none`. Use `effective_capture_event_ids`, `export_json`, `export_markdown` and `tier_counts` from the receipt rather than guessing output paths or counts.

Use the returned foreground JSON/Markdown as the immediate export basis, then apply the existing mastered, bank, A/B/C and grounded-example checks. This output is not a Luna candidate and not a formal-entry candidate.

Do not start a model process, poll the Luna directory or delay the chat response. The background worker discovers the immutable completion event on its next scan.

## Failure

On command or receipt failure, retry once only when the same idempotency key is safe. If it still fails, report that article completion was not durably saved. Do not hand-write the event, invent receipt fields or represent an article projection as the canonical completion result.
