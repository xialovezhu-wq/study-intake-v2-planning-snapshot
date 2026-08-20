# Kaoyan Study Intake Plugin

This personal plugin packages nine versioned Skills and three subject-isolated local read-only MCP namespaces. Root `plugin.json` and `mcp.json` target Agent Plugins 1.0.0. `.codex-plugin/plugin.json` and `.mcp.json` are generated compatibility manifests for the current Codex runtime.

Edit `components.json` and component sources, then run `python3 scripts/generate_manifests.py`. Do not hand-edit generated manifests or `component-lock.json`.

The static plugin manifests register `kaoyan_math_read`, `kaoyan_cs408_read`, and `kaoyan_english_read` as subject-only ordinary servers. The three task-bound Luna executables are dynamically started by the background Host with a private read-session v3 manifest bound to an immutable authority snapshot and are never registered as static ordinary servers. The three background Processing Skills never authorize formal writes. The six interactive Skills use eligibility-driven, subject-qualified MCP-first reads while retaining canonical terminal-only business commands and independent writer verification.
