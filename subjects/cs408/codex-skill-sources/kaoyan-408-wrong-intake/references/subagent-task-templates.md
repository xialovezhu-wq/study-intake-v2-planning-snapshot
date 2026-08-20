# Read-only semantic reviewer templates

Use only when `parallel-intake-contract.md` selects a nonzero role set. Every spawn
uses `fork_turns="none"`. Expand the complete common contract and one role contract
inside the task message; do not ask the reviewer to read skill files.

## Common task contract

```text
Outcome: independently review one bounded answer-safe evidence view and return a
compact structured recommendation.

Inputs:
- snapshot_sha256: {GLOBAL_HASH}
- evidence_view_sha256: {VIEW_HASH}
- route: {A|B|C|F|historical_unmapped}
- evidence_view: {ROLE_SPECIFIC_SAFE_PACKET}

Constraints:
- Read only the supplied packet. Do not inspect conversation history, full tables,
  skills, AGENTS, protected details, screenshots, or attachment paths.
- Do not edit files, run scripts, allocate IDs, touch jobs/WAL/receipts, ask the
  user, or create subagents.
- Do not reconstruct or emit answers, options, complete stems, or explanations.
- Return only the schema below within the stated limits.
```

## Return schema

```json
{
  "snapshot_sha256": "...",
  "evidence_view_sha256": "...",
  "role": "...",
  "status": "ok|blocked",
  "delta": [{"field": "...", "value": "...", "basis": "..."}],
  "evidence": [{"ref": "packet:key", "supports": "..."}],
  "conflicts": [{"field": "...", "kind": "hard|soft", "reason": "..."}],
  "missing": ["..."],
  "warnings": ["..."],
  "write_attempted": false,
  "scripts_invoked": []
}
```

Limits: delta 12, evidence 12, conflicts 4, missing 6, warnings 4; each explanation
at most 160 characters. Do not repeat shared identity fields unless the role owns a
conflict about them.

## Role contracts

### facts_safety

Check provenance, consistency, missing user facts, date/source claims, and answer
safety. Do not choose taxonomy or relationships except to flag unsupported claims.

### taxonomy_error

Choose or challenge exactly one main knowledge point from supplied taxonomy excerpts.
Assess sub/hit knowledge and error tags. Do not infer the user's error from topic
overlap.

### duplicate_relation

Use only the supplied deterministic search packet. Distinguish exact duplicate,
near duplicate, related old node, and unrelated collision. Exact identity or mapping
is a hard conflict and immediate stop. Suggest relation candidates only when the
shared mechanism is explicit; do not run the 7-day recommendation step.

### redo_safety

Check that the redo delta contains only current, answer-safe, user-grounded facts.
Flag any field that would reveal an answer or overwrite unsupported history.

### redo_consistency

Compare the candidate redo delta with the existing node/date/error excerpts. Check
identity, chronological consistency, error-tag merge, and whether the action should
remain redo rather than new.

### relation_semantics

From the two endpoint summaries and supplied R rules, propose at most two relation
types with maximum defensible strength and a concrete shared mechanism. An empty
proposal is valid.

### relation_graph_guard

Act as an independent challenger. Do not receive or anticipate the semantics
proposal. Return admissible relation types, maximum strength, prohibited types, and
endpoint/duplicate/self-loop checks from the frozen packet.

## Main-task acceptance

The main task verifies hashes, schema, limits, role ownership, evidence locations,
and the no-write/no-script fields. Reject invalid or protected output. Merge once;
never let a reviewer call preflight, apply, closeout, or final validation.
