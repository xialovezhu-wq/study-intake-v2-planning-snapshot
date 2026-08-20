# Study Intake V2 Planning Snapshot Manifest

Generated at: `2026-08-20T15:15:33+0800`

History mode: `sanitized-planning-snapshot`

Repository role: Private, non-canonical planning snapshot for ChatGPT Pro review and Terra/Luna replanning.

## Source directories and frozen Git identities

| Part | Source directory | Source branch | Source HEAD |
|---|---|---|---|
| backend | `/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor` | N/A, no independent Git root | N/A |
| shared-mcp | `/Users/xiazhibin/Documents/Codex/local-study-read-mcp` | `main` | `0c54694c417441767f68dba02c8b6a54cae70a7c` |
| subjects/math | `/Users/xiazhibin/Documents/kaoyan-math` | `master` | `c3098894a3611fc0e76cc47985e070b454d0f198` |
| subjects/cs408 | `/Users/xiazhibin/Documents/kaoyan-408` | `codex/recent-408-study-20260715` | `383987141b7ce1eb9e4e190b3b85e0896542f4da` |
| subjects/english | `/Users/xiazhibin/Documents/kaoyan-english` | `master` | `9bbb7c1cf84fbe769cdaa5f38085068fe4b86414` |

The coordination source for handoff documents is `/Users/xiazhibin/Documents/ChatGPT/LunarMax 后台处理的规划`. Its original Git metadata is not included.

## Handoff files

| Snapshot path | Source status | SHA-256 |
|---|---|---|
| `handoff/ORIGINAL_PLAN_HISTORICAL.zip` | COPIED byte-for-byte; historical plan and original design intent only | `d86643dc491132fed79766605c0ea55d6ae13c5f00db64f9fbc657d316b4ce9d` |
| `handoff/Stage-B-Final-Report.md` | `MISSING`; not supplied and not fabricated | N/A |
| `handoff/CURRENT_STATUS.md` | COPIED | `c45a3bc5638d260721800587fcb46773d59a7affdfa6e7d255acc150c2f7a70a` |
| `handoff/WORKSPACE_MANIFEST.yaml` | COPIED | `d46f1f70f334ee3ecc5d6345da1a0ba0a92fe7a56582d9a6a9a36d549e8a578a` |
| `handoff/WORKLOG.md` | COPIED from supplied nested reference path | `9f0d0b3fa1e93675119277a1c1ed7d533303593c904cca2c381271dcf87a3cd6` |
| `handoff/RESUME.md` | COPIED | `a240cee86f5b891fbc98b567d59365cc539d527f269544e63c63763d470b3bd7` |
| `handoff/PUSH_READINESS_REPORT.md` | COPIED | `80c596b677e88196e12f83efe02fbb4e74f6d0a692933de220652588114836d1` |

The older `handoff/WORKLOG.md` contains historical deployment evidence. The newer `handoff/CURRENT_STATUS.md` and `handoff/WORKSPACE_MANIFEST.yaml` are authoritative for the current `PARTIAL` candidate.

`handoff/ORIGINAL_PLAN_HISTORICAL.zip` is historical planning evidence and a record of original design intent. It is not the current final requirement, not a current implementation authorization, and not authoritative when it conflicts with the latest user instructions, the current GitHub code, or `handoff/CURRENT_STATUS.md`. The ZIP was copied without rewriting: 13,825 bytes, SHA-256 `d86643dc491132fed79766605c0ea55d6ae13c5f00db64f9fbc657d316b4ce9d`. It contains two members; archive integrity and the embedded Markdown SHA were verified. The minimum credential, private-key, database, path-traversal, and 50 MiB checks passed.

## Backend whitelist

Only these source-relative paths were copied into `backend/`, with the global exclusions below applied:

- `.gitignore`
- `README.md`
- `CURRENT_STATUS.md`
- `WORKSPACE_MANIFEST.yaml`
- `config.example.json`
- `bin/`
- `dashboard/`
- `launchagents/`
- `lib/`
- `plugin/`
- `schemas/`
- `scripts/`
- `tests/`

Copied result before this manifest and root README: 645 files, 15,005,339 bytes.

## Shared MCP whitelist

Only these current-worktree source-relative paths were copied into `shared-mcp/`, with the global exclusions below applied:

- `.gitignore`
- `README.md`
- `config/`
- `pyproject.toml`
- `requirements.lock`
- `scripts/`
- `src/`
- `tests/`

Copied result: 47 files, 541,042 bytes. The shared MCP `.git/` directory and all original commits were not copied.

## Math whitelist

Exactly 12 source-relative files were copied into `subjects/math/`:

1. `.gitignore`
2. `数学一回滚复习系统/schema/producer-binding-v1.json`
3. `数学一回滚复习系统/schema/quick_intake_events.md`
4. `数学一回滚复习系统/scripts/quick_intake.py`
5. `数学一回滚复习系统/scripts/producer_binding_attestation.py`
6. `codex-skill-sources/kaoyan-math-wrong-intake/SKILL.md`
7. `codex-skill-sources/kaoyan-math-wrong-intake/agents/openai.yaml`
8. `codex-skill-sources/kaoyan-math-wrong-intake/evals/evals.json`
9. `codex-skill-sources/kaoyan-math-wrong-intake/references/immediate-full-closeout.md`
10. `codex-skill-sources/kaoyan-math-wrong-intake/references/rollback-closeout.md`
11. `tests/benchmark_quick_intake.py`
12. `tests/test_quick_intake.py`

Copied result: 12 files, 336,086 bytes.

## CS408 whitelist

Exactly 25 source-relative files were copied into `subjects/cs408/`:

1. `.gitignore`
2. `schema/producer-binding-v1.json`
3. `schema/current-question-evidence-bundle-v3.md`
4. `scripts/intake_fact_capture_408.py`
5. `scripts/producer_binding_attestation_408.py`
6. `scripts/managed_408_current_turn.py`
7. `scripts/current_question_context_408.py`
8. `scripts/current_question_evidence_408.py`
9. `codex-skill-sources/kaoyan-408-wrong-intake/SKILL.md`
10. `codex-skill-sources/kaoyan-408-wrong-intake/agents/openai.yaml`
11. `codex-skill-sources/kaoyan-408-wrong-intake/evals/evals.json`
12. `codex-skill-sources/kaoyan-408-wrong-intake/references/fact-capture-schema.md`
13. `codex-skill-sources/kaoyan-408-wrong-intake/references/intake-checklist.md`
14. `codex-skill-sources/kaoyan-408-wrong-intake/references/intake-package-schema.md`
15. `codex-skill-sources/kaoyan-408-wrong-intake/references/output-template.md`
16. `codex-skill-sources/kaoyan-408-wrong-intake/references/parallel-intake-contract.md`
17. `codex-skill-sources/kaoyan-408-wrong-intake/references/route-map.md`
18. `codex-skill-sources/kaoyan-408-wrong-intake/references/strong-related-review-template.md`
19. `codex-skill-sources/kaoyan-408-wrong-intake/references/subagent-task-templates.md`
20. `tests/test_async_intake_authorization_contract.py`
21. `tests/test_audit_personalization_entrypoints_408.py`
22. `tests/test_daily_study_auto_continuation_contract.py`
23. `tests/test_explicit_study_recording_authorization_408.py`
24. `tests/test_intake_fact_capture_408.py`
25. `tests/test_morning_review_prepared_pack_managed_hot_408.py`

Copied result: 25 files, 706,611 bytes.

## English whitelist

Exactly 20 source-relative files were copied into `subjects/english/`:

1. `.gitignore`
2. `schema/english_pipeline/producer-binding-v1.json`
3. `schema/english_pipeline/capture-event-v2.schema.json`
4. `schema/english_pipeline/capture-receipt-v2.schema.json`
5. `english_pipeline/events.py`
6. `english_pipeline/cli.py`
7. `english_pipeline/producer_binding_attestation.py`
8. `scripts/english_learning_pipeline.py`
9. `codex-skill-sources/kaoyan-english-intensive-reading/SKILL.md`
10. `codex-skill-sources/kaoyan-english-intensive-reading/agents/openai.yaml`
11. `codex-skill-sources/kaoyan-english-intensive-reading/evals/evals.json`
12. `codex-skill-sources/kaoyan-english-intensive-reading/references/candidate-tracking.md`
13. `codex-skill-sources/kaoyan-english-intensive-reading/references/csv-entry-template.md`
14. `codex-skill-sources/kaoyan-english-intensive-reading/references/intensive-reading-output-template.md`
15. `codex-skill-sources/kaoyan-english-intensive-reading/references/quick-capture-contract.md`
16. `codex-skill-sources/kaoyan-english-intensive-reading/references/sentence-output-template.md`
17. `codex-skill-sources/kaoyan-english-intensive-reading/references/sentence-session-workflow.md`
18. `codex-skill-sources/kaoyan-english-intensive-reading/references/tomorrow-review-template.md`
19. `tests/english_pipeline/test_pipeline.py`
20. `tests/english_pipeline/test_quick_flush.py`

Copied result: 20 files, 195,180 bytes.

## Global exclusions

The snapshot excludes all source `.git/` directories and all paths outside the explicit whitelists. Copy filters also exclude:

- `.env`, `.env.*`, `*.key`, `*.pem`, `*.p12`, `*.pfx`
- `*.sqlite`, `*.sqlite3`, `*.db`, `*-wal`, `*-shm`, `*.bak`, `*.backup`
- PDF and image files
- logs and `*.log`
- `__pycache__/`, `*.py[cod]`, test/type/lint caches, `.cache/`, `*.egg-info/`
- `.venv/`, `venv/`, `node_modules/`, `build/`, `dist/`
- `state/`, `private/`, `receipts/`, `outputs/`, `intake/`, `raw/`, `wiki/`
- personal learning data, ledgers, wrong-question cards, original intake, and generated artifacts not explicitly whitelisted

## Known failures and non-blocking partial facts

- `LOCAL_CLOSEOUT_STATUS: PARTIAL`.
- Full Core post-fix: 1144 tests, 1 failure and 1 error, classified as the historical Provider hard-cancel lifecycle race under full-suite load.
- The current central Hermetic Build was not run.
- The current central immutable verify was not run.
- Deployment for the current candidate was not run.
- Some files retain `/Users/xiazhibin` absolute paths as implementation evidence; these are accepted for this Private planning snapshot and are not credentials.
- `Stage-B-Final-Report.md` is missing and was not fabricated.

## Canonical status

This snapshot is not a canonical development repository, not a release artifact, and not evidence of deployment. Future implementation must use the original source workspaces or a formal repository selected by a new authorized plan.
