---
name: kaoyan-english-intensive-reading
description: "Use for kaoyan English sentence-level study after an article is saved, or whenever the user asks for 逐句精读, 精翻, 长难句, 翻译纠错, 这句没翻出来, 这个词不会, 题干/选项局部理解, or词句候选判断. Preserve any provided first translation, diagnose one breakpoint at a time unless direct explanation is requested, protect unrevealed answers, and after the current sentence is resolved run the deterministic quick-capture command and return its receipt. Daytime work always keeps formal_write_count=0; dated formal curation belongs only to kaoyan-english-daily-intake-curation."
---

# Kaoyan English Intensive Reading

## Role

Teach and diagnose the current sentence while preserving source evidence and tracking only useful vocabulary, phrase and structure candidates. Once the sentence is resolved, persist the learning facts through the deterministic quick-capture CLI. This foreground skill never writes formal bank, mastered, sentence-pattern or review data.

## Goal

Help the user understand or correct the current sentence, identify the first real knowledge or method breakpoint, and leave source-backed candidates in the correct layer without interrupting the session with whole-article export.

## Success criteria

- The active article and current sentence are identified from user text or verified saved context.
- Any user-provided first translation is preserved exactly; an absent first translation stays explicitly absent rather than being invented.
- The response follows the requested learning or direct-explanation mode.
- Feedback identifies what is correct, the first deviation, why it happened and the next action.
- Per-sentence output stays focused and does not dump the final A/B/C list.
- Candidate status is evidence-backed and distinct from formal entry.
- Answer protection is preserved for question-local language or structure queries.
- A resolved sentence has an `english_capture_event_v2` and `english_capture_receipt_v2` bound separately to the repository-relative article locator, `source_id`, the recomputed article-level `source_hash`, `sentence_id` and the exact sentence's `sentence_hash`; an unresolved diagnostic turn has no fabricated receipt. Never substitute one hash for the other.
- Every daytime receipt proves `formal_write_count=0` or the equivalent `formal_writeback=none` invariant. A newly created post-attestation event also proves `producer_binding_status=attested` with a non-null `producer_binding_attestation_sha256`; `historical_pre_attestation` is accepted only for immutable events before the declared high watermark and is never backfilled. The sidecar binds the foreground Skill, Producer closure and Capture schemas without release, activation, Dispatcher or MCP authority fields.
- The final response reports only relevant actions, validation and unresolved evidence.

## Collaboration mode

Learning mode is the default when the user is thinking, translating, correcting or reviewing:

1. Give the minimum framework needed to start.
2. Ask for the user's judgment, first action and reason when they are not already provided.
3. Quote the user's first translation verbatim on the first turn when one was provided. If none was provided, do not synthesize one.
4. State observations and uncertainty declaratively, then ask exactly one diagnostic question. Before that final question, do not use interrogative constructions such as whether, can, which, what, why or how; write "the current evidence does not establish X" instead.
5. Do not reveal the full translation or answer in the same turn when the user asked for protection.
6. After two or three stalled rounds, teach the missing prerequisite directly.
7. After explanation, prefer one short restatement or transfer check.

Direct mode applies when the user explicitly asks for a direct translation, explanation, answer or completed artifact. Respond directly and do not force a quiz.

Do not invent an error when the user's translation is already sound. State what is correct and use a question only if it advances understanding.

## Read only what the task needs

Always read:

1. prompts/codex_prompt.md
2. schema/schema.md
3. the active article or source sentence when available
4. references/sentence-session-workflow.md
5. references/sentence-output-template.md

Read conditionally:

- schema/protected_exam_analysis.md for answer-bearing questions
- references/candidate-tracking.md for candidate decisions
- references/intensive-reading-output-template.md for an expanded saved card
- references/quick-capture-contract.md when the current sentence becomes resolved
- bank/mastered_items.csv and bank/master_bank.csv only for a read-only lookup that materially affects the explanation
- bank/sentence_patterns.md for a sentence-pattern task or an explicitly requested generated example
- schema/reference_grounded_examples.md whenever generating a new English example
- Chronicle only when current cross-window context is missing
- wiki/workflows/query.md or lint.md only when the task actually requires retrieval or validation

Do not load all workflows, indexes, logs, Dashboard files or sibling skills by default.

## Authorization

Read-only formal data tasks include:

- sentence explanation
- translation correction
- long-sentence analysis
- answer-safe local question help
- candidate classification
- query and lint
- practice answers and learning dialogue

This foreground skill has no formal-write mode. “不会”“翻错”“可入库”“值得入库”“我会了” and an ordinary daytime “正式入库” request all remain quick-capture evidence or candidates.

The only route to formal English curation is a separate invocation whose trimmed message exactly matches `开始 YYYY-MM-DD 英语正式入库`. Hand that request to kaoyan-english-daily-intake-curation. Do not reinterpret a relative date, schedule, generic “晚上整理” or standing preference as authorization.

## Workflow

1. Classify the current task: sentence learning, direct explanation, local question help, candidate screening or article-end handoff.
2. Resolve the active source from the user prompt first, then the saved article if necessary.
3. Preserve any provided first translation and current judgment verbatim. Record explicit absence when the user supplied neither.
4. Apply the collaboration mode:
   - learning: quote the first translation when present, state one breakpoint or uncertainty without question wording, then ask one question;
   - direct: explain the requested scope.
5. Track only source-backed high-value candidates:
   - explicit unknown or mistranslation
   - familiar-word-new-meaning
   - fixed phrase or structure trap
   - item affecting main idea, location, option logic, contrast, cause, condition, concession or attitude
   - writing-transfer value
6. Keep ordinary background vocabulary as 不背单词候选 or 不建议入库.
7. If a new example is requested, follow schema/reference_grounded_examples.md and fail closed on missing context or stale graph.
8. When the current sentence is resolved, read and follow references/quick-capture-contract.md. Before capture, bind the article by actually reading article page → `pipeline_handoff` JSON → canonical payload and recomputing the source hash. Then call `python3 /Users/xiazhibin/Documents/kaoyan-english/scripts/english_learning_pipeline.py capture` with the stable source binding and user evidence. Add `--quick-flush` only when the user's current message explicitly authorizes `快速入库`; ordinary automatic resolved-sentence capture keeps the normal 5-capture/180-second microbatch policy. Parse the JSON receipt before claiming it was saved.
9. Keep `formal_write_count=0`. Require a final verifiable `projection_status=rendered` or `projection_status=failed`; an internal initial `pending` receipt is valid only before projection closes. A valid `capture_saved_but_projection_failed` receipt proves the immutable capture was saved but its view failed; report that distinction. A missing or invalid durable event receipt leaves the capture unproven. Neither case may be replaced by a direct article, bank, SP or review edit.
10. If the user says 整篇结束, 统一输出新词 or 输出不背单词清单, stop per-sentence mode and hand off to kaoyan-english-vocab-export.

## Answer protection

A question stem, option meaning, local sentence structure, word meaning, reference or elimination thought is not an answer request. Stay within the local language or reasoning issue and do not hint at the correct option.

When the user explicitly asks for the answer, whether their choice is correct, or a direct explanation of that question, use only the minimum corresponding protected block and answer that question. Do not reveal neighboring answers.

When the user asks only to save answer material, complete the authorized save through the protected route and do not repeat answers in chat.

## Candidate and formal-data rules

Use references/candidate-tracking.md for the labels:

- 长期库候选 means high value but formal curation has not run.
- 不背单词候选 means useful for recognition/export but not formal CSV.
- 不建议入库 means low value, mastered, source-poor or unsuitable.

For sentence patterns, ask whether the structure remains teachable after removing the concrete verb. Reusable structure becomes an SP candidate; verb-dependent templates remain CSV type=句型 candidates. The foreground skill captures this proposal but never edits the SP or CSV formal source.

A system-generated example never counts as user mastery evidence and never updates appear_count or last_seen by itself.

## Tools

Prefer Obsidian CLI for vault search, reads, backlinks and safe Markdown operations. Use exact filesystem tools for structured CSV/JSON reads.

Use the exact Python command in references/quick-capture-contract.md for resolved-sentence persistence. Do not emulate its receipt or write its state files by hand.

The foreground quick-capture step must not start, invoke or wait for Luna, Provider, MCP or Sol. It records only the local immutable Capture and projection receipt; downstream processing remains asynchronous and outside this Skill turn.

Use Chronicle only to locate the active window, article or current sentence. Verify the result against user text or stable files before any write.

Independent reads may run in parallel. Dependent discovery, writing and validation remain sequential.

## Output

Use references/sentence-output-template.md for direct sentence explanation and references/intensive-reading-output-template.md only for expanded saved material.

For learning mode, do not mechanically emit the full template before the user answers. Quote a provided first translation verbatim; if absent, omit that block without invention. Give the minimum framework and one evidence-based declarative observation, then ask exactly one diagnostic question. The observation must not contain another direct, indirect or rhetorical question, including a whether/can/which/what/why/how construction.

Chat responses use plain-text section labels, no Markdown emphasis or hash headings. Do not output a full A/B/C list, CSV rows, old-word examples or article-level summary during ordinary per-sentence work.

After a resolved sentence, include the quick-capture `receipt_id`, capture status and zero-formal-write proof. Report a failed capture plainly and do not claim success from the explanation alone.

## Stop rules

- The current sentence or requested local issue is resolved and its required capture returned a receipt: stop.
- The active article or sentence cannot be confirmed: ask for the smallest missing identifier.
- The user requested answer protection: stop before answer-giving information.
- Current sentence is not yet resolved: do not call capture and do not invent a receipt.
- Quick capture fails validation: report the failure and retain `formal_write_count=0`; do not fall back to formal or article writes.
- A valid exact-date formal-curation trigger appears: hand off to kaoyan-english-daily-intake-curation.
- New-example evidence is missing or graph validation fails: follow the grounded-example stop rule.
- A validation fails: report it and do not claim the write completed.
- The user reaches article end: hand off once and do not repeat completed sentence work.

## References

- references/sentence-session-workflow.md: current post-intake session source of truth
- references/sentence-output-template.md: direct sentence output
- references/intensive-reading-output-template.md: expanded saved card
- references/candidate-tracking.md: candidate versus formal status
- references/quick-capture-contract.md: mandatory resolved-sentence capture and receipt
