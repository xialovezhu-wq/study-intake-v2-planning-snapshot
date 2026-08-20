# Study Intake V2 Planning Snapshot

This repository is a Private planning snapshot for ChatGPT Pro code review, status auditing, and a new Terra/Luna follow-up plan.

It is not a completed release, not a deployed release, not the formal history migration of any source project, and not the canonical development repository. No original Git history from the backend, shared MCP, math, CS408, English, or coordination workspace is included.

## Current status

- `LOCAL_CLOSEOUT_STATUS` remains `PARTIAL`.
- The latest full Core run still has the historical Provider hard-cancel lifecycle race: 1 failure and 1 error in 1144 tests.
- The current central Hermetic Build and immutable verify were not run after that result.
- Deployment for the current candidate was not run.
- Some copied files intentionally retain `/Users/xiazhibin` absolute paths as implementation evidence. Those paths are not treated as credentials for this planning snapshot.
- `handoff/Stage-B-Final-Report.md` is `MISSING`: no local file was supplied, and no report was reconstructed from summaries.
- `handoff/ORIGINAL_PLAN_HISTORICAL.zip` preserves the historical plan and original design intent exactly as supplied. It is not the current final requirement and does not grant current implementation authorization. If it conflicts with the latest user instructions, the current GitHub code, or `handoff/CURRENT_STATUS.md`, it is not authoritative.
- The supplied `handoff/WORKLOG.md` is an older reference worklog and includes historical deployment statements. For current status, `handoff/CURRENT_STATUS.md` and `handoff/WORKSPACE_MANIFEST.yaml` take precedence.

## Purpose and boundary

Use this snapshot to understand code, audit the current partial state, and design the next Terra/Luna work plan. Any later code implementation must return to the original source workspaces or to a formal repository explicitly selected by a new plan.

The source projects were not modified while creating this snapshot. No tests, Build, verify, runtime deployment, source-project Git initialization, source-project commit, source-project remote change, or source-project push was performed.

See `SNAPSHOT_MANIFEST.md` for exact sources, whitelists, exclusions, hashes, and known gaps.
