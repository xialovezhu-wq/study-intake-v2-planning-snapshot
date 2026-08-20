# Current-question intake checklist

## Fast capture

- [ ] One validated current-question context and one opaque grader capsule are bound.
- [ ] The local study date/timezone and stable source SHA-256 are explicit.
- [ ] Feedback was frozen before any write.
- [ ] The first-answer and necessary session receipts bind this exact item.
- [ ] Eligibility matches `current_question_failure_standing_policy_v1`.
- [ ] The private evidence manifest/object/attachments pass hash, path, owner, and
      permission checks.
- [ ] Bundle v3 uses exactly `dialogue_only|image_question`; image rows preserve
      roles/order and pass declared/detected MIME, magic, byte-count, and SHA-256
      checks; one-to-eight rows are accepted and a ninth is rejected.
- [ ] Trace v2 binds original/included/omitted counts, omitted ranges, reason, and
      full-trace SHA-256 within the 24-event and 32-KiB limits.
- [ ] `evidence_pending` performs zero model enqueue; only a verified ready handoff
      can reach the fixed Luna Max two-pass pipeline.
- [ ] The public capture contains only answer-safe facts, private locator/hashes,
      provenance, gaps, date, IDs, idempotency, and receipts.
- [ ] At most one capture exists and its terminal is `awaiting_daily_curation`.
- [ ] Formal preflight/apply, relation lookup, old-question lookup, global audit,
      personalization, Luna, and manual successor lookup counts are zero; any next
      surface came only from the unified answer result.
- [ ] Failure returns the exact recovery surface without duplicating outcome/session.

## Sol curate-one

- [ ] The user supplied an explicit date and the capture belongs to the exact frozen
      ordered batch/set hash.
- [ ] The raw capture payload and private manifest/bundle were independently reopened.
- [ ] The current immutable release consumer was called exactly once; its report is
      optional and hash-verified.
- [ ] A consumable report is `two_pass_ready`; both `analysis` and
      `critical_review` receipts independently confirm `gpt-5.6-luna` at Max.
- [ ] Sol independently verifies the current release and its HMAC authority chain;
      self-declared authority fields are not trusted.
- [ ] Every Luna candidate has one Sol adopt/modify/reject decision with direct
      evidence, basis, counterevidence, confidence, and gaps.
- [ ] Immutable conflicts yield `needs_user` and apply count zero.
- [ ] New identity keeps `formal_id=null` until RepoLock allocation; redo binds one
      verified existing ID.
- [ ] Exactly one formal apply ran, or `already_current` has an exact verification
      SHA-256.
- [ ] Full postcommit and audits completed before `mark-result` and before the next
      item starts.
- [ ] A prior normal receipt is resumed, never replayed.
- [ ] Adoption/workbench failures cannot roll back or substitute for formal truth.
