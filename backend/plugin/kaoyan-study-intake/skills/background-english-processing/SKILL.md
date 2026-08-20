---
name: background-english-processing
description: "Internal model-driven MCP Luna preprocessing contract for one immutable kaoyan English intensive-reading microbatch. Use only when the Study Intake host binds this Skill and an English-only read session. It preserves complete source sentences and user evidence while proposing vocabulary, phrase, structure, and same-day status decisions without formal bank writes."
---

# Background English Processing

Read `../../references/shared-processing-contract.md` completely before processing.

Skill version `4.0.0`.

## Active Multi-Agent V2 route

When `processing_binding_v3` is present, this Skill governs exactly one Luna reader branch. Terra owns read-plan creation, branch selection, fan-in, candidate integration and fresh-context review. This Luna session owns one `branch_id`, one independent read session and one independent subject MCP launcher; it cannot delegate. Later references to a single task-wide Analysis/Critical Review pair describe legacy replay compatibility and are inactive under the V2 binding.

## Evidence preservation

Preserve the complete article identity, full source sentences, user first translation, corrected meaning, teaching explanation, candidates, and user evidence verbatim. Do not replace them with a summary. The absence of a signal is not mastery evidence. Generated examples are proposals, never source sentences or user facts.

## Legacy two-pass compatibility route

The Host dynamically starts `study-read-mcp-english --stdio --read-session-manifest <absolute-v4-json>` as server `kaoyan_english_read`. The exact allowed tool surface is `get_task_context`, `read_task_artifact`, `list_records`, `get_records`, `search_records`, and `query_relations`. Do not use an ordinary bundle, a legacy `english_investigate_library` tool, another subject namespace, terminal, shell, arbitrary path, SQL, plugin, web, or writer tool.

The bootstrap contains only this Skill binding, capture ID, read-session binding, release and Schema hashes, and output instructions. It contains no article prose, sentence summary, master-bank match, mastered-item match, pattern shortlist, grounding packet, or article-history shortlist. At the start of Analysis, call `get_task_context`, then read every returned artifact ID with `read_task_artifact`, following every text-artifact cursor until complete. Then use at least one of the four library tools and whichever additional library tools are needed for article identity, vocabulary, phrase, pattern, event, and relation judgments.

Select the necessary collections yourself from `article_catalog`, `sentences`, `vocabulary`, `mastered_items`, `patterns`, `events`, and `search`. Use `ids` for exact item scope; optional `query` filters any collection. `page_size` must be 1..48, never 50. Query the complete safe library as needed and follow every short opaque `next_cursor` until `complete=true` by copying it exactly and keeping all other query arguments unchanged; never edit or synthesize a cursor. Ground existing/new decisions only in rows actually returned to this stage.

For this English Skill only, this subject-specific rule takes precedence over the shared contract's generic `output limit` fail-closed sentence. An `OUTPUT_LIMIT` result from a library exploration is a failed non-evidence call, but it is not terminal when the same information need is subsequently recovered by a distinct call with a smaller `page_size`. Do not repeat the identical arguments, and keep every non-size query argument unchanged. The smaller-page chain is recovered only when every returned `next_cursor` is copied exactly until `complete=true` and `next_cursor=null`, and every output judgment cites only successful current-stage calls. The failed call remains raw-transport evidence only and contributes no grounding, duplicate, coverage, or evidence ledger entry. A recovered chain must not create a blocking finding or correction resolution and must not force Critical Review to reject solely because the earlier call failed. If `page_size=1` still returns `OUTPUT_LIMIT`, a cursor remains unresolved, the smaller call changes another query argument, or required evidence remains insufficient, fail the stage closed. Starting a distinct smaller-page MCP call is not a second Provider submission and does not relax the exactly-one Analysis and exactly-one fresh Critical Review contract.

Critical Review runs in a fresh model context bound to the immutable Analysis draft hash. It must independently call `get_task_context`, reread every task artifact to completion, and call at least one library tool that supports or refutes a key draft judgment. It cannot approve from the draft alone. Analysis and Critical Review bind the same generation and authority but have distinct MCP transcripts.

For an ordinary processing attempt, the Host must validate the exact provider request before submission: `--ignore-user-config`, `--model gpt-5.6-luna`, and `model_reasoning_effort="max"`; a `service_tier` override is forbidden. A mismatch fails before the provider. Submit Analysis exactly once and fresh Critical Review exactly once. If either submission fails, stop and sign the failed attempt; do not retry, downgrade, or add a third submission. Record `requested_service_tier=null`, `fast_mode_requested=false`, and `fast_mode_effective=not_requested`. A deterministic or zero-model replay is regression evidence only and never counts as either real stage.

Subject verification may start a real smoke attempt only for a newly frozen, distinct foreground intensive-reading capture with new event bindings and a new unit, batch, Dispatcher runtime, HMAC key, and read session. It must not reuse or split an earlier business capture, use an inventory object, or substitute an already-current placeholder task for a real capture.

For every Analysis item and every Critical Review item, `grounding.mcp_evidence_refs` must contain both of these stage-local evidence roles:

- at least one exact `mcp-item:english:<sha256>` ref returned by `read_task_artifact` for a task artifact whose pagination reached `complete=true`; and
- at least one exact `mcp-item:english:<sha256>` ref returned by a successful, relevant library read in that same stage.

A `get_task_context` ref does not satisfy either role. A Critical Review item cannot use an Analysis-stage ref as evidence of Critical Review consumption. A failed tool result and a successful zero-item or zero-hit exploration produce no grounding evidence and cannot be cited. Do not invent, normalize, or carry a ref from the other stage as proof of current-stage consumption. Protected or cross-subject data remain unavailable.

## Matching and same-day protection

Split every explicit vocabulary, phrase, mistranslation, unresolved structure, and reusable-pattern signal into one terminal outcome. An immutable capture that contains at least one explicit signal must never produce an empty proposal item set. Analysis must emit one terminal outcome for every explicit signal, and Critical Review must return a nonempty `revised_items` array that preserves that complete coverage. If MCP evidence proves that a term or pattern is already current, keep a schema-valid item and record the applicable existing-state marker, such as `bank_status=existing_bank` or `mastered_status=mastered_excluded`, instead of dropping it or inventing a new operation. Otherwise propose a new candidate or request review.

Any explicit unknown, mistranslated, prompted, or unresolved-structure signal protects the term for the local study day. Later silence or vague correct use cannot remove protection. Preserve all conflicting events. Only an explicit user correction or explicit mastery statement may produce a release-protection proposal; it never performs reactivation.

Mastered-items, master bank, sentence patterns, article records, and review state remain unchanged. Optional pattern hits are search hints, not automatically adopted patterns.

## Authorized existing-object recuration

When the Host binds the one-shot legacy-recuration profile, the task is one immutable, already-authorized existing English object. Read the bound target-evidence artifact completely, call `get_records` for the exact stable ID, and use `search_records` or `query_relations` to test duplicate identity. This profile never authorizes an ordinary English task or a different inventory.

Analysis may return only `update_existing_proposal`, `already_current_proposal`, `conflict`, or `evidence_incomplete`. Critical Review must start from a fresh model context, reread the artifact and exact formal target through MCP, and return `accepted`, `corrected`, or `rejected`. Existing-object recuration forbids create, replacement ID, merge, delete, or automatic identity repair. A matching current object remains `already_current_proposal`; it still receives independent evidence and a quality receipt, but no formal operation.

Every attempt is explicit. A successful target uses exactly one Analysis submission followed by exactly one Critical Review submission. An interrupted or failed attempt can only continue as a new recorded attempt; do not hide an exploratory third submission. Luna publishes proposal-only packages and never claims Sol review or formal application.

## Review expectations

The reviewer checks atomic-signal coverage, complete sentence preservation, source separation, same-day protection, actual MCP grounding, answer safety, mastery overclaim, and unsupported generated content. Every blocking correction has one stable owner and exact canonical path set. It must already be applied in revised items; otherwise reject.

For every card, `old_word_example` must be either an empty string or a newly composed English example. After whitespace normalization and case folding it must not equal the bound source sentence. Never copy, lightly re-punctuate, or case-change the source sentence into this generated-example field; the source sentence is already preserved by the host-owned record.

Copy every source event ID exactly from the frozen batch. A ready package has one terminal outcome per defensible explicit signal, full coverage, and no unmatched signal.

## Output boundary

Return proposal-only data compatible with `luna_proposal_v2`. The host binds the untouched revised items to the capture-freeze receipt, both stage transcripts and grounding manifests, and the final HMAC read-session receipt without host semantic projection. Do not modify vocabulary banks, mastered items, patterns, articles, review status, or any formal object; do not call Sol or claim a completed write. `formal_write_count=0`.
