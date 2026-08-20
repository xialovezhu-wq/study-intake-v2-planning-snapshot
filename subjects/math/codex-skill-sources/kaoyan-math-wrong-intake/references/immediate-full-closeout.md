# Immediate Full Closeout

Read this reference only when the user explicitly asks to finish the current question's formal card, wrongnet rebuild, and rollback synchronously now. Ordinary 入库、更新、记录复发、标记掌握 requests during study use fast capture and return immediately.

This is a batch-size-one closeout-v2 workflow, not a legacy bypass. It must first create the same immutable capture used by the daytime path, freeze that exact event before formal writes, and finish every closeout-v2 gate. The only routing difference from nightly QA is that the user explicitly chose to wait for the whole pipeline now.

## Boundary

Process exactly one real source-backed current question with one Agent. Exclude earlier questions in the same long task. Preserve confirmed correct steps, the first reusable break, later distinct method/concept/calculation breaks, hints, repairs, unresolved points, and mastery evidence.

Every claim keeps provenance. User behavior requires direct user evidence. Assistant explanations can establish mathematical correctness but cannot be rewritten as independent user behavior. Solution-only inference remains `model_inferred_from_solution` or pending.

Read `数学一回滚复习系统/schema/quick_intake_events.md` before creating freeze or closeout payloads. The closeout receipt always uses `model: unknown`; do not create or override model-provenance environment variables.

## Phase 1: Capture And Freeze

1. When this was a scoreable warmup item, persist the canonical score first and retain its stable score event ID.
2. Use the main skill's exact `math-fast-intake-capture-v1` contract and deterministic `quick_intake.py record` writer to append one capture for the current episode. Do not return after `pending_nightly`; continue only because the user explicitly authorized synchronous closeout.
3. Resolve the formal identity without scanning unrelated history. For a real new source, persist the verified source artifact inside the repository, verify its hash, check duplicates, and allocate an ID only after re-reading the current subject maximum.
4. Before any formal or derived write, create a `math-fast-intake-freeze-v1` payload containing exactly that capture and one target resolution. Set `scope: explicit_subset`; use `existing_formal`, `new_source_created`, or `new_source_merged` according to verified identity.
5. Run:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py freeze \
  --payload-file /private/tmp/math-fast-intake-immediate-freeze.json \
  --consume-payload-file
```

6. Use only the returned `freeze_id` and canonical `formal_bindings` tokens. A changed or created formal card must contain the exact frozen `fast_intake_refs`; a new-source target must also contain its exact `fast_intake_source_refs` token.

If record or freeze fails, stop before formal writes. Do not replace the missing durable event with an informal note.

## Phase 2: Serial Formal And Derived Closeout

Use the current episode, exact target card, applicable template/rule sections, necessary method or knowledge registry section, and at most two or three identity comparators. Keep every semantic decision and write serialized.

1. Patch the formal card once, including the frozen structured references, and repair every verified visual mapping needed by that card.
2. Run exactly one successful final `python3 错题知识网络/scripts/wrongnet.py rebuild`; allow one retry only for a diagnosed transient failure.
3. Perform the eligible targeted rollback upsert and verification. For wrong or recurrence outcomes, the per-capture durable record must bind the real processed rollback event and current unit hash. Representation or mastery outcomes use the schema-defined formal-card or closeout binding and must not invent a rollback event.
4. Keep formal relations in SHADOW. Produce the per-target proposal required by closeout-v2 without writing an unauthorized formal edge.
5. Complete the required Wiki ingest, targeted lint, and target parity for the formal ID. Pass the current wrongnet snapshot's validated `updated_at` as parity `--date`; this artifact date may differ from the study date. Wiki is mandatory in this immediate full route; never leave it for a future nightly task.
6. Verify every current visual reference and artifact hash. Use `not_applicable` only when the final formal card contains no visual reference.

If rebuild fails, stop before rollback, relationships, Wiki, or closeout. Any later gate failure leaves the capture pending and retryable; never manufacture history or a passed receipt merely to finish synchronously.

## Phase 3: Closeout-v2

Create one `math-fast-intake-closeout-v2` receipt for the returned freeze. It must contain:

- one exact `formal_results` item;
- one exact `capture_results` item with a schema-compatible durable record;
- `batch_receipts.wrongnet` with the nine current artifacts and target projection hash;
- the per-target SHADOW relationship decision;
- the current Wiki source-summary path and hash;
- the target visual receipt;
- `model: unknown`.

There is no separate `batch_receipts.rollback` key. The capture result's durable record carries the required rollback or schema-defined no-event binding.

Run:

```bash
python3 数学一回滚复习系统/scripts/quick_intake.py close \
  --receipt-file /private/tmp/math-fast-intake-immediate-closeout.json \
  --consume-receipt-file
```

## Completion

Use `立即完整正式入库完成` only when `close` returns `recorded` or `noop`. Report the formal ID, actual changed fields, one successful rebuild declaration, rollback outcome, relationship state, Wiki state, visual state, closeout ID, and receipt model `unknown`.

If any gate remains incomplete, name that layer and state that the capture remains pending. Do not describe a formal/rebuild/rollback-only result as complete, and do not append old-card recommendations.
