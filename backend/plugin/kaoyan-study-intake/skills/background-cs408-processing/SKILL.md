---
name: background-cs408-processing
description: "Internal model-driven MCP Luna preprocessing contract for one immutable kaoyan 408 capture in morning_review or formal_problem. Use only when the Study Intake host binds this Skill and a 408-only read session. It proposes item, knowledge, error, and relation changes while preserving event facts and reserving every formal write for explicitly authorized 408 Sol curation."
---

# Background 408 Processing

Read `../../references/shared-processing-contract.md` completely before processing.

Skill version `4.0.0`.

## Active Multi-Agent V2 route

When `processing_binding_v3` is present, this Skill governs exactly one Luna reader branch. Terra owns read-plan creation, branch selection, fan-in, candidate integration and fresh-context review. This Luna session owns one `branch_id`, one independent read session and one independent subject MCP launcher; it cannot delegate. Later references to a single task-wide Analysis/Critical Review pair describe legacy replay compatibility and are inactive under the V2 binding.

## Scene and evidence gate

`scene` is exactly `morning_review` or `formal_problem`. Morning review preserves review identity, display receipt, first answer, correction trace, exposed problem, and confidence provenance. Formal problem preserves stable question identity, question image, solution image, complete dialogue, and actual user answer. Quick intake is immutable fact capture; “put into the wrong-question library” means `propose_new_item`, not a completed write.

## Legacy two-pass compatibility route

The Host request uses `--ignore-user-config`, `--model gpt-5.6-luna`, and `model_reasoning_effort="max"`; a `service_tier` override is forbidden. Record `requested_service_tier=null`, `fast_mode_requested=false`, and `fast_mode_effective=not_requested`.

The Host dynamically starts `study-read-mcp-cs408 --stdio --read-session-manifest <absolute-v4-json>` as server `kaoyan_cs408_read`. The exact allowed tool surface is `get_task_context`, `read_task_artifact`, `list_records`, `get_records`, `search_records`, and `query_relations`. Do not use an ordinary bundle, a legacy `cs408_investigate_library` tool, another subject namespace, terminal, shell, arbitrary path, SQL, plugin, web, or writer tool.

The bootstrap contains only this Skill binding, capture ID, read-session binding, release and Schema hashes, and output instructions. It contains no question prose, images, dialogue summary, knowledge snapshot, taxonomy shortlist, historical wrong-item shortlist, direct-edge shortlist, or semantic findings. At the start of Analysis, call `get_task_context`, then read every returned artifact ID with `read_task_artifact`, following every text-artifact cursor until complete. Read bound images as MCP ImageContent. Then use at least one of the four library tools and whichever additional library tools are needed for identity, projection/event authority, knowledge, and relation judgments.

Select the necessary collections yourself from `knowledge_catalog`, `knowledge_nodes`, `relationships`, `search`, `curation_inventory`, `morning_sessions`, and `review_events`. Use `ids` for exact node or review scope; optional `query` filters any collection. `page_size` must be 1..48, never 50. Follow every short opaque `next_cursor` for each selected query until `complete=true`: copy it exactly, keep all other query arguments unchanged, and never edit or synthesize a cursor. Use stable IDs, publication identity, or verified review identity first; otherwise search by content/source evidence.

`state.json` is a projection. Its event-ledger binding must pass before any Luna call. Projection/event mismatch fails closed and is never repaired or reinterpreted by the model.

Critical Review runs in a fresh model context bound to the immutable Analysis draft hash. It must independently call `get_task_context`, reread every task artifact to completion, and call at least one library tool that supports or refutes a key draft judgment. It cannot approve from the draft alone. Analysis and Critical Review bind the same generation and authority but have distinct MCP transcripts. Every historical comparison or relation proposal must be grounded in an item actually returned to that stage. Each stage copies exact `mcp-item:cs408:<sha256>` refs only from rows it actually consumed; a ref returned only to the other stage proves nothing for the current stage.

## Semantic decisions

Return existing, new candidate, or `identity_ambiguous`. Split knowledge, method, first error, later error, trap, and question type into atomic signals. Existing formal knowledge is marked; missing fine-grained knowledge becomes `propose_new_knowledge`.

Query parent/child knowledge and same-problem relations only as needed. Existing old-node edges remain source facts. Any relation involving the current capture is `propose_relation` until 408 Sol independently adopts it.

A study observation is not automatically a wrong-item candidate. Prompted or fragile correctness is not independent mastery. Private analysis may use bound answer evidence, but formalization candidates remain answer-safe.

## Provider and canonical validation

The provider schema stays stable and portable; it does not dynamically enumerate evidence references or correction paths. The canonical host validator checks exact reference membership against actual MCP results, correction paths, generation, authority, answer safety, proposal-only verbs, and all required correction resolutions.

Finding identifiers are unique. A blocking correction must be materially applied to the complete revised analysis or the verdict is `reject`. No old package or host-owned projection is appended to a failed stage.

## Output boundary

Return proposal-only data compatible with `luna_proposal_v2`. The host binds the untouched model semantics to the capture-freeze receipt, both stage transcripts and grounding manifests, and the final HMAC read-session receipt without host semantic projection. Do not start a batch, allocate IDs, create nodes, link edges, mark formal results, prepare morning content, call Sol, or claim completion. `formal_write_count=0`.
