# Conditional semantic-review contract

This is the single source of truth for Codex subagent selection in formal 408
maintenance. Subagents are used for independent quality checks, not as a speed
claim. The deterministic postcommit process DAG is separate.

Read `../../_shared/native-luna-parallel-contract.md`. Every role in this contract is
a native `explorer` leaf pinned to `gpt-5.6-luna`, reasoning effort `max`, and
service tier `fast`, with `fork_turns="none"`. Reject results whose execution
metadata does not confirm all three fields. These roles never invoke or substitute
for the separate background Study Intake Luna worker or consumer.

`fast_capture` always uses zero subagents and never reads this matrix to justify a
delay. It preserves stable facts and explicit gaps. The matrix applies only to
`full_now`, a dated `curate_one`, or an authorized formal maintenance route.

## Exact identity gate

Before any new-node fan-out, the main task checks formal ID, historical source-ID
mapping, locator mapping, and details ID. An exact existing match selects redo,
narrow maintenance, or `needs_user`; it never starts new-node roles.

## Selection matrix

| Condition | Roles |
|---|---|
| exact existing | none |
| A low ambiguity | none |
| A substantive quality risk | `facts_safety`, `taxonomy_error`, `duplicate_relation` |
| B sufficient and low ambiguity | none |
| B high risk | `facts_safety`, `taxonomy_error`; main task owns duplicate/relation |
| B insufficient evidence | none; degrade to D |
| C simple redo | none |
| C complex redo | `redo_safety`, `redo_consistency` |
| F otherwise | none |
| F disputed type/strength plus quality risk | `relation_semantics`, `relation_graph_guard` |
| D/E/G | none |
| evidence-complete unmapped historical item | `facts_safety`, `taxonomy_error`, `duplicate_relation` |

Substantive quality risk means uncertainty in source/history mapping, answer safety,
the unique main knowledge point, near-duplicate status, redo consistency, or relation
evidence. Task length, model tier, or a desire to demonstrate Ultra is not risk.

Prior local benchmarking found no stable semantic-agent speedup for A/C/F; keep the
matrix quality-driven. Low-ambiguity work remains direct.

During dated daily curation, independent read-only semantic preparation for several
captures may run concurrently on frozen answer-safe views. Formal writes remain
strictly serial and batch size 1. Agent count is not evidence that a stronger model
was used; report model tier or reasoning effort only when execution metadata confirms
it.

Create one child for each role selected by the matrix for each naturally independent
frozen capture or bounded capture shard. Do not enforce a Skill-level numeric cap;
fill available runtime slots and continue the remaining roles in waves. All selected
results meet at one fan-in barrier, and Sol merges exactly once before serial apply.

## Snapshot and role views

The main task creates one normalized answer-safe snapshot and
`snapshot_sha256`. Each role receives:

- the same global hash;
- its own `evidence_view_sha256`;
- only the minimum role-specific evidence;
- the role contract and bounded return schema;
- no surrounding conversation through `fork_turns="none"`.

Role views:

- `facts_safety`: verified user/source facts and safe metadata; no protected details.
- `taxonomy_error`: safe facts plus relevant knowledge/error taxonomy excerpts.
- `duplicate_relation`: a deterministic bounded search packet covering candidate
  nodes, mappings, locators, knowledge hits, and adjacent edges.
- `redo_safety`: current user facts and answer-safety-sensitive redo fields.
- `redo_consistency`: existing node/date/error excerpts plus candidate redo delta.
- `relation_semantics`: two endpoint summaries, relevant R rules, and adjacency.
- `relation_graph_guard`: the same independent evidence but no semantics proposal;
  it returns admissible types, maximum strength, prohibited types, and
  endpoint/duplicate/self-loop checks.

Never send complete stems, answers, options, screenshots, handwriting, protected
paths, whole rulebooks, or whole tables.

## Bounded response

Use `subagent-task-templates.md`. Every response must include the global/view hashes,
status, compact delta, evidence references, conflicts, missing facts, warnings,
`write_attempted=false`, and `scripts_invoked=[]`. Reject a result with a wrong hash,
protected content, file mutation, script use, or fields outside the role.

## Main-task fan-in

Wait for all started roles and merge once in the main task. Do not run peer voting or
a second critique round.

Hard conflicts include identity, source mapping, duplicate status, new/redo, user
facts, main knowledge point, answer leakage, and illegal relation endpoints. They
must be resolved before preflight or apply; otherwise apply count is zero.

Frozen user facts, source/details identity, study dates, and evidence hashes cannot
be changed by fan-in. A reviewer disagreement becomes `needs_user` or a dropped
model-derived field, never an overwrite of the capture fact block.

Soft candidates such as sub-knowledge, topic chain, or optional relation may be
dropped conservatively. For F, cross-check the semantic proposal against the
independent graph guard; completion order must not change the result.

## Failure and stop

If a role times out, violates contract, or a slot is unavailable, discard the
invalid result and perform that check serially in the main task. Do not replace it
with another role's vote. If the hard fact remains unresolved, stop closed.

Subagents never edit files, run repository scripts, allocate IDs, touch jobs/WAL/
receipts, ask the user, or launch more agents. Preflight, apply, closeout, and
related-candidate generation remain in the main task and local deterministic tools.
Receipt-bound closeout must never reapply or rerun semantic agents.
