# Post-Intake Sentence Session Contract

## Goal

Resolve the current sentence at the user's requested depth, preserve any provided first translation, diagnose the first real breakpoint, and keep candidates separate from formal writes. A missing first translation stays absent.

## Entry

Use after a saved article is identified or when the user provides the complete current sentence and enough local context.

If neither the sentence nor the active article can be confirmed, ask only for the article path, title, year/Text or exact sentence needed to proceed.

## Mode

Learning mode:

1. Give the smallest useful framework.
2. Use the user's current judgment and first action as evidence.
3. Quote the user's first translation verbatim on the first turn only when one was provided.
4. State what is already correct.
5. Locate the first uncertain word, structure or method step in declarative wording.
6. Ask exactly one diagnostic question; before it, do not preview the diagnostic with a whether/can/which/what/why/how construction or another indirect or rhetorical question.
7. Wait for the answer before increasing the hint level.
8. After two or three stalled rounds, explain the missing prerequisite directly.

Direct mode:

- If the user asks for direct translation, explanation or answer, provide it immediately within the requested scope.
- Do not force a quiz or restatement before answering.

Answer-protected mode:

- Do not reveal or imply a correct option.
- A stem, option translation, local structure, word meaning or elimination thought is not an answer request.
- Explicit requests for the answer, correctness judgment or direct question explanation unlock only the requested question.

## Per-sentence work

1. Preserve the user's first translation exactly when present; never invent one to satisfy capture fields.
2. Work only on the current sentence.
3. Depending on mode, diagnose or explain:
   - natural meaning
   - sentence backbone
   - clauses and modifiers
   - logic and reference
   - key collocations
   - the first evidence-backed user breakpoint
4. Track only useful candidates.
5. Do not output a full BBDC list, A/B/C export, CSV row set or unrelated old-word examples.
6. Do not capture after an intermediate diagnostic question. Once the sentence is resolved by direct explanation, correction or completed teaching, validate the real article → pipeline_handoff → canonical payload chain, then call the deterministic CLI in quick-capture-contract.md and return its final v2 receipt.
7. When the user reaches article end and requests a unified list, hand off once to kaoyan-english-vocab-export.

## Candidate status

- Explicit unknown or mistranslation: high-priority candidate unless mastered, a proper name or clearly low value.
- Familiar-word-new-meaning, fixed phrase, structure trap or writing-transfer expression: long-term candidate.
- Ordinary background vocabulary: BBDC candidate or not recommended.
- Reusable structure: SP candidate.
- Verb-dependent template: CSV type=句型 candidate.

Candidate value does not authorize a formal write.

A resolved sentence with no observed unknown, mistranslation or useful candidate is still a valid zero-candidate capture. Do not invent a breakpoint or evidence to make the event nonempty.

## Foreground persistence

The daytime path persists only immutable `english_capture_event_v2` learning facts and `english_capture_receipt_v2` projection state through `english_learning_pipeline.py capture`. It never writes master_bank, mastered_items, sentence_patterns or review files, and it does not start or wait for Luna, Provider, MCP or Sol. Generic formal-entry language does not widen this scope; dated formal curation uses the separate exact trigger and skill.

## Generated examples

Any new English example uses schema/reference_grounded_examples.md. Missing source_sentence returns needs_context. Missing current user wording and explicit unknown, mistranslated or missed evidence returns needs_user_evidence. Stale graph is rebuilt and verified once. At most two compliant foundation packets may be tried; then fail closed.

## Output

Learning mode outputs only:

- the user's first translation verbatim on the first turn when present
- one evidence-based observation
- the current breakpoint or uncertainty
- one diagnostic question

Direct mode uses references/sentence-output-template.md, trimmed to the user's scope.

After a resolved sentence, append the real capture receipt block from quick-capture-contract.md. An unresolved learning turn omits it.

Chat labels are plain text. Do not use Markdown emphasis or hash headings.

## Stop rules

- Current question resolved and capture receipt returned: stop.
- One learning question asked: wait.
- Answer protection active: stop before answer-giving information.
- Resolved sentence returned no valid durable event receipt: report capture as unproven without editing another file as fallback. A valid `capture_saved_but_projection_failed` receipt is saved evidence with a failed view, not an unsaved capture.
- Active source missing: ask for the smallest identifier.
- Validation failed: report it without claiming completion.
- Article-end export requested: hand off and stop per-sentence work.
