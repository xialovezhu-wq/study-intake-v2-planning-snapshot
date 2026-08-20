---
name: kaoyan-english-vocab-export
description: "Use for article-end or stage-end kaoyan English vocabulary export whenever the user says 整篇结束, 统一输出新词, 输出不背单词清单, 生成不背单词 App 导入模板, or asks for A/B/C vocabulary tiers. First call the deterministic complete-article command and return its receipt, then immediately produce the source-backed output-only A/B/C list without starting or waiting for Luna. Keep formal_write_count=0; dated formal curation is a separate exact-trigger skill."
---

# Kaoyan English Vocab Export

## Role

Durably mark the active article complete, then turn its foreground capture snapshot into an output-only BBDC or 不背单词 A/B/C list. This skill does not wait for background preprocessing and does not perform formal entry.

## Goal

Produce a complete, source-backed export that the user can review or import, while excluding mastered and duplicate items and preventing generated examples from bypassing the four-layer evidence gate.

## Success criteria

- The active article or source is confirmed.
- `complete-article` returns a real receipt for the exact article and effective capture set before completion is claimed.
- Candidates come from the article, user-marked unknowns, mistranslations or verified sentence notes.
- mastered_items exclusions occur before master_bank matching.
- Duplicate and existing-bank status are reported.
- A/B/C classification follows the current priority rules.
- Every new old-word linkage example either passes the four-layer contract or fails closed.
- Source sentences and reference IDs are not invented.
- No formal bank, sentence-pattern, article or review file is modified.
- The completion and export receipts prove `formal_write_count=0`; Luna remains asynchronous and is not a prerequisite for A/B/C output.
- The final output preserves all required card fields without unrelated process narration.

## Read only what the task needs

Always read:

1. prompts/codex_prompt.md
2. schema/schema.md
3. the active article or source page
4. bank/mastered_items.csv
5. bank/master_bank.csv
6. references/abc-priority-rules.md
7. references/bbdc-export-format.md
8. references/output-template.md

Read conditionally:

- schema/reference_grounded_examples.md whenever generating an old-word linkage or any new English example
- references/memory-curve-old-word-selection.md when selecting old words
- bank/sentence_patterns.md only when a natural SP candidate is needed
- today's memory-curve derived files only after verifying freshness against current formal sources
- wiki/workflows/query.md for source or old-word retrieval
- wiki/workflows/lint.md for duplicate, source and boundary checks
- Chronicle only when the active article or current stage is missing from the prompt
- the intake or intensive-reading skill only when a handoff boundary is unclear

Do not load every sibling skill, index, log, Dashboard or second-brain file by default.

## Authorization

Default behavior is output-only.

This skill must not modify:

- bank/master_bank.csv
- bank/mastered_items.csv
- bank/sentence_patterns.md
- any review file
- real article data
- Dashboard or second-brain data

If the user later requests formal curation, finish and present the output-only export first. A separate exact-date message invokes kaoyan-english-daily-intake-curation; this skill never routes formal work back through intensive reading.

Do not treat article completion as permission for formal curation. Only the exact trimmed message `开始 YYYY-MM-DD 英语正式入库` invokes kaoyan-english-daily-intake-curation for that one date.

## Workflow

1. Confirm article-end or stage-end export intent.
2. Resolve the active source from an explicit path, title, year/Text, recent verified context or user-provided filename.
3. Read references/article-completion-contract.md. Call `python3 /Users/xiazhibin/Documents/kaoyan-english/scripts/english_learning_pipeline.py complete-article` with the canonical state directory, stable article ID and a stable idempotency key. Parse the JSON receipt before claiming completion.
4. Use the returned effective capture snapshot plus verified article evidence to build the foreground A/B/C output immediately. Do not invoke, poll or wait for Luna; Luna candidates are for the later dated curation flow.
5. Read mastered_items first and exclude matches from A/B/C.
6. Read master_bank and classify each remaining candidate as new, existing, duplicate-like or needs checking.
7. Apply references/abc-priority-rules.md.
8. For each A or B card:
   - preserve the real source sentence;
   - write article-specific meaning and usage;
   - add a short review note;
   - generate an old-word linkage example only when useful.
9. For every generated example:
   - read schema/reference_grounded_examples.md;
   - require source_sentence and a fresh selector result;
   - use approved or corrected作文句型与词组 plus a verified syllabus occurrence;
   - keep SP optional;
   - try at most two different compliant foundation packets;
   - otherwise report needs_context, needs_user_evidence, needs_reference_graph or 待审核参考缺口 without an example.
10. Select old words through references/memory-curve-old-word-selection.md. Prefer due and natural active items; use oldest-fallback only when necessary; never use mastered items.
11. Produce the output and state that dated formal curation is a separate next step.
12. Run completion-receipt, duplicate, source, grounding and zero-write checks.

## Four-layer generated examples

The detailed source of truth is schema/reference_grounded_examples.md. The export format must carry:

- 用户措辞或错词依据
- 作文句型来源和审核状态
- 作文词组来源和审核状态
- 大纲 occurrence 和核验状态
- 可选 SP
- 例句与中文
- 结构和自然度检查

If any required evidence is missing, omit the final generated example rather than filling fields from model memory.

The original article sentence remains source evidence. A generated old-word example never replaces it and never updates mastery, appear_count or last_seen.

## Tools

Use Obsidian CLI for article and wiki discovery when available. Use exact file tools for CSV comparison, hashes and structured validation.

Use only the deterministic command in references/article-completion-contract.md to record completion. Do not hand-write an `article_completed` event or simulate its receipt.

Use Chronicle only to identify the likely active article or stage, then verify against stable files. Chronicle cannot determine A/B/C, mastery, source sentence or formal write status.

Independent reads may run in parallel. Candidate classification follows source resolution and mastered/bank checks.

## Output

Use references/output-template.md and references/bbdc-export-format.md.

Chat responses use plain-text section labels, no Markdown emphasis or hash headings. Preserve complete card fields for each included item, but omit empty global sections that add no information.

At minimum report:

- source
- complete-article receipt and effective capture count
- candidate counts
- A/B/C cards
- mastered and duplicate exclusions
- master_bank match status
- grounding status for generated examples
- formal-data zero-write status
- asynchronous Luna status without waiting
- next exact-date formal-curation handoff

## Stop rules

- Active article cannot be identified: ask only for path, title or year/Text.
- `complete-article` fails or its receipt cannot be verified: report completion as unsaved and do not fabricate an A/B/C snapshot receipt.
- Candidate has no real source sentence: mark needs_context and keep it out of A/B generated-example output.
- Candidate has neither current user wording nor explicit unknown, mistranslated or missed evidence: mark needs_user_evidence and omit the generated example.
- All relevant items are mastered: report exclusions and stop without filling tiers.
- Graph is stale: rebuild and verify once, then retry selector once.
- Two compliant foundation packets fail naturalness: report 待审核参考缺口 and omit the example.
- A valid exact-date formal-curation trigger is requested: complete this output-only stage, then hand off to kaoyan-english-daily-intake-curation.
- Validation fails: report the failed check and do not claim export completion.

## References

- references/abc-priority-rules.md: A/B/C classification
- references/bbdc-export-format.md: per-item fields
- references/memory-curve-old-word-selection.md: active old-word selection
- references/article-completion-contract.md: deterministic completion event and foreground snapshot
- references/output-template.md: final output

## Local read-only MCP route

The deterministic `complete-article` command and verified receipt run first and remain terminal-only. After completion succeeds, eligibility is true for cold evidence covered by the English-only MCP binding server `kaoyan_english_read`, tool `english_read_bundle`, exposed in Codex as `mcp__kaoyan_english_read__english_read_bundle`: exact article sentences, master-bank status for known candidates, needed patterns, and coverage. When eligible, that fully qualified tool is mandatory MCP-first for every chunk. Global namespaces `mcp__kaoyan_read__english_read_bundle`, `mcp__kaoyan_read_v2__english_read_bundle`, and `mcp__kaoyan_read_v3__english_read_bundle` are legacy and must not be called. The math and 408 namespaces are cross-subject and must not be called. Mastered-items, business rules, old-word selection, completion, capture, Luna polling, formal curation, and write-foundation validation remain terminal-only.

Every MCP request must use `schema_version=study-read-mcp.v3` and route context for caller `kaoyan-english-vocab-export`, Skill version `2.1.0`, the installed plugin version, a unique route request ID, one canonical evidence-scope hash, ordered chunk metadata, and `consumed_duplicate_read_count=0`. Deterministically split more than 24 stable IDs; every chunk must bind the same article, generation, and authority fingerprint. Any failed or mismatched chunk invalidates the entire route.

Before the first eligible call, check the callable task snapshot for the exact required tool `mcp__kaoyan_english_read__english_read_bundle`. If that exact tool is absent, legacy or cross-subject namespaces do not satisfy the requirement: call no MCP tool, record `read_route=terminal_fallback`, `required_mcp_tool=mcp__kaoyan_english_read__english_read_bundle`, `fallback_reason=tool_snapshot_missing`, `chunk_index=0`, and `chunk_count=0`, then immediately run the whole existing evidence route without probing, retrying, or restarting Codex. On a missing field or any other failure, drift, inconsistency, timeout, or limit from the required English tool, discard all MCP chunks and rebind the complete covered scope to one fresh canonical terminal generation. Never mix MCP and fallback evidence.

After MCP success, consumed terminal duplicate reads for MCP-covered article, bank, pattern, event, or projection evidence must be zero. Terminal-only sources remain required and must be labeled separately. The MCP result never replaces the completion receipt or mastered-items authority and must report `formal_write_count=0` and `model_call_count=0`.
