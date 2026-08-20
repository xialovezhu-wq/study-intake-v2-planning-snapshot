# Targeted Rollback Closeout

Use only when the current card is a recurrence, has evidence-backed mastery 0–2, or has at least three errors and should enter high-priority long-term review.

1. Confirm authoritative `exam_date` exists when creation or recurrence requires scheduling.
2. Preview the current ID only:

```bash
python3 数学一回滚复习系统/scripts/scheduler.py upsert-wrongnet --id FORMAL-ID --date YYYY-MM-DD --dry-run
```

For a formal recurrence, add `--expect-recurrence` to both preview and apply.

3. Verify canonical unit ID, action, source version, priority, occurrence identity, and `anchor_eligibility_date`.
4. Apply the same command without `--dry-run`.
5. Accept only `回滚同步完成` or a verified same-source `noop`.

Never replace this route with bulk `sync-wrongnet` or `add`. The eligibility date means the gap may be checked from that date; it does not promise redelivery of the original card. Warmup separately selects an eligible formal old card under its seven-day delivery gate.
