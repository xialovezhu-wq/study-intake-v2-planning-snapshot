---
name: multi-agent-read-orchestrate
description: Release-bound Terra orchestration SOP for one immutable Study Intake task. It plans any number of independent read branches, delegates only to Luna leaf readers, waits for every required branch, preserves failures and conflicts, and never performs formal writes.
---

# Study Intake Multi-Agent Read Orchestrate

Skill version `1.0.0`.

## Input boundary

Read only the frozen task context, task artifact manifest, immutable release binding, authority snapshot and generation supplied by the Host. Do not read a formal subject database directly and do not trust deployment identity declared by Capture content.

## Plan

Create one `orchestration_read_plan_v1` object before spawning readers.

- Split only semantically independent read domains.
- Give every branch one unique ID, purpose, rationale, bounded read scope, allowed tools, dependency list, completion requirements and failure policy.
- Preserve every logically necessary branch. Physical capacity may queue branches in waves but must never delete, merge or truncate them.
- Keep cursor chains, pagination, `search_records` to `get_records`, and all response-dependent calls serial within one branch.
- Reject cycles, duplicate branches, cross-subject scopes, writer tools, terminal, shell, web and SQL.

## Delegation

Every reader is a leaf using `gpt-5.6-luna` with reasoning effort `max`, read-only sandbox and agents disabled. Each branch receives an independent child identity, read-session manifest and subject-scoped STDIO MCP launcher. Readers must not delegate again.

## Fan-in

Wait for every required branch to reach a terminal state. Preserve failed, cancelled, timed-out and missing branches. Build one content-addressed read bundle with ordered request/result hashes, MCP receipts, evidence membership, deduplication, conflicts, coverage and authority/generation closure.

## Integration and review

Integrate only claims grounded in the read bundle. A fresh-context Terra reviewer reads the frozen task, read plan, read bundle, candidate and receipts. It does not automatically reopen the subject database.

Allowed review outcomes are `accepted`, `corrected`, `issues_found` and `technical_quarantine`.

- `issues_found` remains execution success, is visible to Sol, preserves candidate and risk report, and does not automatically retry.
- `technical_quarantine` preserves diagnostics but does not expose a trusted candidate.
- Quality issues from one item must not hide or block clean items.

## Safety

Terra and Luna never write formal study data. Every artifact and receipt has `formal_write_count=0`. Final apply remains exclusively controlled by an independently authorized Sol writer transaction.
