# Current-question and formal-maintenance route map

Choose the time phase before A-G. Do not combine routes merely because they touch
the same question or formal ID.

## Phase decision

1. Active current-question learning: the managed turn applies the standing-policy
   matrix and conditionally writes only the fast capture before returning to study.
2. A capture frozen by the explicit-date daily-curation skill: `curate_one`, then choose
   A/B/C from the current evidence.
3. Existing formal maintenance or read-only review: choose D-G directly.

`fast_capture` never reads the A/B shared formal preread set, allocates an ID, runs
formal preflight/apply, creates relations, looks up old questions, or starts global
audits. Identity or main knowledge may remain an explicit capture gap. It completes
only with a durable evidence locator and capture receipt.

## Decision order

1. Existing formal node was answered incorrectly again: C.
2. User asks only whether intake is possible, or material facts are missing: D.
3. Only details, a safe card, or bridge is being repaired: E.
4. Only a relation edge is being added or corrected: F.
5. Only an old-question review or candidate list is requested: G.
6. One new formal node: A when durable details exist, B when evidence is only
   temporary or the durable details entry is pending.

An explicitly named historical WAL/receipt recovery is outside A–G and belongs to
the legacy coordinator.

## Shared formal new-node preread

For formal A/B under `curate_one`, satisfy the project `AGENTS.md`
new-question curation contract. Use targeted
searches or bounded sections for large tables, but cover every required source:
master nodes, edges, year index, knowledge taxonomy and hit index, concept and error
taxonomies, relation rules, subject node/knowledge files, relevant topic chains,
and reference indexes when needed.

Do not substitute a model summary for these local sources. Do not read unrelated
full tables or every external learning skill.

## A: new node with durable details

- Verify the details card belongs to the same question.
- Run exact identity gates before semantic classification.
- Build one new-mode package and use the normal orchestrator.
- Write no dated review file.
- Use conditional semantic reviewers only when the quality contract selects them;
  a fast capture never starts them.

## B: new node with temporary evidence

- Never store temporary attachment paths in formal fields.
- Mark the durable details entry as pending when absent.
- Year, subject, question identity, main knowledge point, and user error entry must
  be supported. If a material field is not supported, degrade to D with zero writes.
- Use the same exact identity gate and normal orchestrator as A.

## C: redo of an existing node

- Read the exact master row, safe card, relevant date/error history, and current
  user evidence.
- Update the existing node; never allocate a new node or silently add relation edges.
- A simple redo is handled directly. Use independent semantic checks only for a
  real conflict about user-fact safety or consistency with the existing node.
- Run the normal single-question orchestrator and current full closeout.

## D: read-only feasibility or needs_user

- Check exact identity and the smallest relevant taxonomy/history evidence.
- Optionally run deterministic preflight in read-only form.
- Return the candidate route, supported fields, blocking gaps, and the smallest
  question that would unblock intake.
- Write nothing and do not reserve a final ID.

## E: details, safe-card, or bridge maintenance

- Read only the named formal node, details card/assets, safe card, and bridge state.
- Preserve answer-safe separation.
- Details-only changes may use the dedicated details/asset tools. If a formal row or
  safe card must change, require a supported narrow single-writer transaction. The
  current new/redo package must not be misused to simulate that maintenance; if no
  suitable writer exists, stop with a tooling blocker instead of editing directly.
- Refresh bridge and run ID-scoped detail/HTTP validation as applicable.
- Do not touch unrelated indexes, edges, or dated reviews.

## F: relation-only maintenance

- Read both formal endpoints, existing matching edges, and relevant relation rules.
- Historical IDs cannot be endpoints. Reject self-loops and duplicates.
- Low-evidence relationships may end as `reviewed_no_reliable_edge`.
- Use an independent semantics/challenger pair only for a substantive dispute over
  type or strength; otherwise decide directly.
- For a full validated semantic-review set, the supported WAL writer is
  `apply_relation_semantic_review.py publish --apply`. It is not a single-edge
  shortcut. For an isolated edge, use only a repository-supported narrow writer;
  if none is available, return the reviewed decision without direct table mutation.
- Run network-health validation after an authorized transactional write.

## G: related old-question review

- Use `scripts/related_candidates_408.py --id ID` or its knowledge-point form.
- Trust the script for date filtering, historical location lookup, and redaction,
  then format with `strong-related-review-template.md`.
- Write nothing. Do not create edges from a recommendation.

## Stop conditions

Stop with zero formal writes when identity, source mapping, new/redo, user facts,
unique main knowledge point, answer safety, or permission remains unresolved. Stop
after the selected phase and route's validation; do not opportunistically perform
another route. A valid fact capture may preserve these gaps without formal writes,
but it must not describe itself as a completed formal node.
