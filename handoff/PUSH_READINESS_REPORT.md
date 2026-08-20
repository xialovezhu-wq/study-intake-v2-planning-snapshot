# Study Intake V2 Push Readiness Report

生成时间：2026-08-20 +08:00

审核性质：只读 GitHub push readiness 审核。未执行测试、`git init`、`git add`、`git commit`、仓库创建、remote 修改或 `git push`。除本报告外，未写入任何文件。

## 1. 审核结论

总体结论为 `PARTIAL`，当前不允许执行任何目标仓库的 push。

主要原因：

1. 中央后台仍是 `LOCAL_CLOSEOUT_STATUS: PARTIAL`；完整 Core 为 1 failure、1 error，中央 Hermetic Build 和 immutable verify 均为 `NOT_RUN`。
2. 中央候选中有 49 个文件包含 `/Users/xiazhibin` 本机绝对路径，需在首次发布前决定改为可移植路径或显式接受私有仓库中的本机身份披露。
3. 共享 MCP 的当前树和全部可达历史安全扫描通过，但当前仍有 20 个 tracked unstaged 修改和 16 个 untracked 文件，且 GitHub 目标仓库与 remote 尚不存在。
4. 三科必须使用独立、无原历史的 sanitized snapshot；候选 descriptor、部分测试或 Skill 文档含本机绝对路径，尚未生成和复核净化后的 staging tree。
5. 五个拟议 GitHub 仓库均未查询到，实际 visibility 和 default branch 尚无法验证。
6. `Stage-B-Final-Report.md` 没有本地文件；冻结说明明确阶段 B 最终报告只存在于原会话回复中，本报告未伪造该文件。

## 2. 输入与冻结核对

| 文件 | 结果 | SHA-256 |
|---|---|---|
| `CURRENT_STATUS.md` | MATCH | `c45a3bc5638d260721800587fcb46773d59a7affdfa6e7d255acc150c2f7a70a` |
| `WORKSPACE_MANIFEST.yaml` | MATCH | `d46f1f70f334ee3ecc5d6345da1a0ba0a92fe7a56582d9a6a9a36d549e8a578a` |
| `WORKLOG.md` | MATCH | `21ddd5aceadba711f2fe93b4bddb62ab44cdff8fe89cc0d20dae42ca74cfd5c8` |
| `RESUME.md` | MATCH | `a240cee86f5b891fbc98b567d59365cc539d527f269544e63c63763d470b3bd7` |
| `Stage-B-Final-Report.md` | MISSING_LOCAL_FILE | 不适用 |

`CURRENT_STATUS.md` 和 `WORKSPACE_MANIFEST.yaml` 均为普通文件，不是符号链接。`WORKLOG.md` 和 `RESUME.md` 也均为普通文件。四份冻结文件的高置信 token/private-key 扫描为零命中。

## 3. 仓库映射总表

| 目标 | 来源目录 | 预计仓库名 | 预计 visibility | 建议默认分支 | history mode | 预计 remote | 当前允许 push |
|---|---|---|---|---|---|---|---|
| 中央后台 | `/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor` | `study-intake-v2-backend` | Private | `main` | `sanitized-snapshot` | 新仓库内 `origin` -> `https://github.com/xialovezhu-wq/study-intake-v2-backend.git` | NO |
| 共享 MCP | `/Users/xiazhibin/Documents/Codex/local-study-read-mcp` | `local-study-read-mcp` | Private | `main` | `preserved-history` | 当前根未来 `origin` -> `https://github.com/xialovezhu-wq/local-study-read-mcp.git` | NO |
| 数学 | `/Users/xiazhibin/Documents/kaoyan-math` | `kaoyan-math` | Private | `main` | `sanitized-snapshot` | 独立 staging repo 的 `origin` -> `https://github.com/xialovezhu-wq/kaoyan-math.git` | NO |
| 408 | `/Users/xiazhibin/Documents/kaoyan-408` | `kaoyan-408` | Private | `main` | `sanitized-snapshot` | 独立 staging repo 的 `origin` -> `https://github.com/xialovezhu-wq/kaoyan-408.git` | NO |
| 英语 | `/Users/xiazhibin/Documents/kaoyan-english` | `kaoyan-english` | Private | `main` | `sanitized-snapshot` | 独立 staging repo 的 `origin` -> `https://github.com/xialovezhu-wq/kaoyan-english.git` | NO |
| 协调目录 | `/Users/xiazhibin/Documents/ChatGPT/LunarMax 后台处理的规划` | 默认不建仓库 | 不适用 | 不适用 | attachment-only | NONE | NO |

GitHub API 只读核对结果：上述五个拟议仓库在 `xialovezhu-wq` 下均未查询到。因此 Private 和 `main` 是创建时的强制建议，不是已验证的远端事实。

remote 策略纠正：三科原 Git 根当前只有 Gitee `origin`。本轮迁移原则禁止从原根 push、禁止给原根添加 GitHub remote，因此不采用旧 manifest 中“原根增加 `github` remote”的建议。后续只能在根外建立 sanitized staging repo，并在 staging repo 使用 GitHub `origin`；三科原 Gitee `origin` 保持不变。

## 4. 中央后台

### 4.1 来源与 history mode

- 来源：`/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor`
- 当前不是独立 Git 根。
- 建议：`sanitized-snapshot`，建立新 Private 仓库时只复制白名单，不继承外层历史。

### 4.2 精确白名单

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

目录项必须再应用下列精确排除规则；不得 blanket-copy 根目录。

### 4.3 精确排除

- 根级：`config.json`、`release.json`、`logs/`、`state/`、`packages/`、`private/`、`receipts/`、`outputs/`、`validation/` 中非白名单生成物。
- 类型或模式：`**/__pycache__/`、`**/*.py[cod]`、`.DS_Store`、`.pytest_cache/`、`.mypy_cache/`、`.ruff_cache/`、`.venv/`、`venv/`、`node_modules/`、`.env`、`.env.*`、`*.pem`、`*.p12`、`*.key`、`*.sqlite`、`*.sqlite3`、`*.db`、`*-wal`、`*-shm`、`*.bak`、`*.backup`、`build/`、`dist/`。
- freeze marker：`validation/source-freeze-sha256-*.txt`。
- 任何不在 4.2 白名单中的根路径。

### 4.4 文件级与历史级扫描

- 候选展开：807 entries，748 regular files，59 directories。
- 内容扫描：748/748 可读；3 个压缩 fixture payload 也做了不回显内容的凭据扫描。
- secret/token/JWT/bearer/credential URL/private-key PEM：零命中。
- 环境文件、key、数据库、WAL、SHM：白名单内零命中。
- 私有运行数据：发现 8 个 `__pycache__` 目录、103 个 `.pyc`，合计 15,463,404 bytes；均受 `.gitignore` 排除，不得进入 snapshot。
- 符号链接：0。
- 大文件：超过 10 MiB 为 0，超过 50 MiB 为 0；候选中单文件最大为 1,457,106 bytes 的已排除 `.pyc`。
- 本机绝对路径：49 个候选文件包含 `/Users/xiazhibin`。这是 portability/privacy 风险，不是已发现的凭据。
- 历史扫描：不适用；目标采用新 snapshot，不允许继承外层 Git 历史。

### 4.5 阻塞项与 push 判定

- `LOCAL_CLOSEOUT_STATUS: PARTIAL`。
- 完整 Core 仍有 1 failure、1 error。
- 中央 Hermetic Build 和 immutable verify 未运行。
- 49 个本机绝对路径尚未净化或显式接受。
- GitHub 仓库尚不存在，visibility/default branch 未实证。

ALLOW_PUSH: NO

## 5. 共享 MCP

### 5.1 来源与 history mode

- 来源：`/Users/xiazhibin/Documents/Codex/local-study-read-mcp`
- branch：`main`
- HEAD：`0c54694c417441767f68dba02c8b6a54cae70a7c`
- refs：仅 `refs/heads/main`；无 remote、tag 或 upstream。
- 建议：`preserved-history`。必需的 secret/private-key/database/large-blob 历史扫描已通过。

### 5.2 精确白名单

- `.gitignore`
- `README.md`
- `config/`
- `pyproject.toml`
- `requirements.lock`
- `scripts/`
- `src/`
- `tests/`

### 5.3 精确排除

- `.venv/`
- `work/`
- `**/__pycache__/`
- `**/*.py[cod]`
- `**/*.egg-info/`
- `.DS_Store`
- `.pytest_cache/`、`.mypy_cache/`、`.ruff_cache/`、`.cache/`
- `node_modules/`
- `.env`、`.env.*`
- `*.pem`、`*.p12`、`*.key`
- `*.sqlite`、`*.sqlite3`、`*.db`、`*-wal`、`*-shm`
- `*.bak`、`*.backup`
- `build/`、`dist/`
- 任何不在 5.2 白名单中的新工作树路径。

### 5.4 文件级扫描

- 候选扫描：94 regular files，1,267,826 bytes；该计数保守地包含候选目录内已忽略的 cache/egg-info，实际提交必须继续受 `.gitignore` 约束。
- worktree：20 tracked unstaged 修改，16 untracked 文件，0 staged。
- secret/token/password/private-key：零命中。
- SQLite/database/WAL/SHM：零命中。
- 符号链接：0。
- 大文件：超过 10 MiB 为 0；当前候选最大 74,484 bytes。
- 本机路径：当前候选中 5 个逻辑文件含 `/Users/xiazhibin`：`README.md`、`config/skill-canary-patches.json`、`scripts/build_release.py`、`src/study_read_mcp/config.py`、`scripts/shadow_validate.py`。这是非秘密的 portability/privacy 风险。

### 5.5 全部可达历史扫描

- 范围：`git rev-list --objects --all` 的全部对象。
- 计数：4 commits、18 trees、40 blobs，共 62 unique reachable objects。
- 40/40 blobs 内容级 secret/token/password/private-key 扫描：零命中。
- 数据库、SQLite header、WAL/SHM 路径或内容：零命中。
- 大 blob：超过 10 MiB 为 0，超过 50 MiB 为 0；最大 blob 为 16,905 bytes，object `8ef4143f0199d2049ebd95eb6143f6be34e56e0c`，路径 `src/study_read_mcp/adapters/cs408.py`，风险 LOW。
- 历史中的 4 个 commit 均有 5 个逻辑路径含 `/Users/xiazhibin`：`README.md`、`config/codex-mcp-snippet.toml`、`config/skill-canary-patches.json`、`scripts/shadow_validate.py`、`src/study_read_mcp/config.py`。不含凭据，但保留历史会保留该本机路径披露。
- unreachable、reflog-only、stash-only 和未来 commit 不在 `--all` 范围内。

### 5.6 阻塞项与 push 判定

- 安全扫描允许建议保留当前可达历史。
- 当前目标状态尚未形成提交，36 个工作树变化不能被当作可达历史已审核完成后的正式 commit。
- 目标 GitHub 仓库、Private visibility、remote 均尚不存在。
- 保留历史意味着同时保留本机绝对路径；发布前需显式接受该低到中风险，或改选 clean snapshot。

ALLOW_PUSH: NO。安全历史门已通过；完成未来授权的内容确认、commit、Private 仓库创建和 remote 配置后才可重新判定。

## 6. 数学 sanitized snapshot

### 6.1 精确白名单

来源：`/Users/xiazhibin/Documents/kaoyan-math`

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

依据：descriptor/capture/source closure 与 canonical Skill 共 9 项，策略要求 `.gitignore` 1 项，直接引用测试 2 项。

### 6.2 排除与扫描

- 精确排除：除上述 12 个相对路径外的全部内容。
- 特别禁止：原 `.git/` 历史、`*.pdf`、图片、`数学一回滚复习系统/快速入库来源/`、`快速入库事件.jsonl`、`复习记录.jsonl`、`学习前5题记录.jsonl`、`错题知识网络/错题卡/`、`错题知识网络/生成/`、`.codex_tmp/`、环境文件、key/PEM、数据库、WAL/SHM、缓存、备份和生成物。
- 12/12 存在，12/12 为普通文件，0 symlink，5/5 descriptor hash 检查匹配。
- 高置信 token/private-key/credential/email/phone/政府证件：零命中。
- 禁用类型：0。超过 10 MiB：0。
- privacy finding：`producer-binding-v1.json` 和 `tests/test_quick_intake.py` 含本机绝对用户路径规则命中。
- 原历史不迁移。历史已知含约 19.4 MB 生成 JSON blob，不能镜像。

### 6.3 staging 与 push 判定

在原 Git 根外新建无历史 staging tree，仅复制上述 12 项；将本机绝对路径改为安全的 repo-relative/configurable 表达并重新做 hash/secret/type/size 检查。原 Gitee `origin` 不变，原根不增加 GitHub remote。

ALLOW_PUSH: NO

## 7. 408 sanitized snapshot

### 7.1 精确白名单

来源：`/Users/xiazhibin/Documents/kaoyan-408`

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

依据：descriptor/capture/source closure 与 canonical Skill 共 18 项，策略要求 `.gitignore` 1 项，直接引用测试 6 项。

### 7.2 排除与扫描

- 精确排除：除上述 25 个相对路径外的全部内容。
- 特别禁止：原 `.git/` 历史、`wiki/study_vaults/408-full/state/`、任何 `.key`、SQLite/WAL/SHM、`CN_*.md`、`DS_*.md`、`OS_*.md`、`CO_*.md`、`复习单元卡/`、dated dashboard queues、benchmark output、书籍/PDF/图片、个人 wiki、题目、复习卡、账本、运行态、缓存、备份和生成物。
- 25/25 存在，25/25 为普通文件，0 symlink，8/8 descriptor hash 检查匹配。
- 高置信 token/private-key/credential/email/政府证件：零命中。
- 禁用类型：0。超过 10 MiB：0。
- privacy finding：`schema/producer-binding-v1.json` 含本机绝对用户路径；phone-like 数字规则命中需人工确认，未输出内容。
- 原历史不迁移。已知历史文件名含 18 个 `.key` 和 26 个 SQLite 路径，禁止镜像。

### 7.3 staging 与 push 判定

在原 Git 根外新建无历史 staging tree，仅复制上述 25 项；移除或参数化本机绝对路径，人工裁决 phone-like 规则命中，再重新做 hash/secret/type/size 检查。原 Gitee `origin` 不变，原根不增加 GitHub remote。

ALLOW_PUSH: NO

## 8. 英语 sanitized snapshot

### 8.1 精确白名单

来源：`/Users/xiazhibin/Documents/kaoyan-english`

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

依据：descriptor/capture/source closure 与 canonical Skill 共 17 项，策略要求 `.gitignore` 1 项，直接引用测试 2 项。

### 8.2 排除与扫描

- 精确排除：除上述 20 个相对路径外的全部内容。
- 特别禁止：原 `.git/` 历史、`intake/`、`tmp/`、`deferred-intake/`、`raw/`、`articles/`、`wiki/`、`bank/mastered_items.csv`、PDF/图片、个人 bank、账本、运行态、环境文件、key/PEM、数据库、WAL/SHM、缓存、备份和生成物。
- 20/20 存在，20/20 为普通文件，0 symlink，8/8 descriptor hash 检查匹配。
- 高置信 token/private-key/credential/email/phone/政府证件：零命中。
- 禁用类型：0。超过 10 MiB：0。
- privacy finding：`schema/english_pipeline/producer-binding-v1.json`、canonical Skill `SKILL.md`、`references/quick-capture-contract.md` 含本机绝对用户路径。
- 原历史不迁移。已知历史文件名含 4 个 SQLite、2 个 WAL、2 个 SHM 路径，禁止镜像。

### 8.3 staging 与 push 判定

在原 Git 根外新建无历史 staging tree，仅复制上述 20 项；移除或参数化本机绝对路径，再重新做 hash/secret/type/size 检查。原 Gitee `origin` 不变，原根不增加 GitHub remote。

ALLOW_PUSH: NO

## 9. 协调目录与附件

默认不建立 GitHub 仓库，不迁移当前协调目录的本地 Git 元数据、旧报告、`evidence/`、private MCP transcript、解压包、ZIP、原型、脚本或生成物。

建议新会话附件精确清单：

1. `/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor/CURRENT_STATUS.md`
2. `/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor/WORKSPACE_MANIFEST.yaml`
3. `/Users/xiazhibin/Documents/ChatGPT/LunarMax 后台处理的规划/WORKLOG.md`
4. `/Users/xiazhibin/Documents/ChatGPT/LunarMax 后台处理的规划/RESUME.md`
5. `/Users/xiazhibin/Documents/ChatGPT/LunarMax 后台处理的规划/PUSH_READINESS_REPORT.md`

`Stage-B-Final-Report.md` 不在附件清单中，因为本地文件不存在。若未来确需独立持久化，应由拥有原会话报告正文的会话另行生成并提供 SHA；本审核不从摘要反向伪造报告。

## 10. 执行 push 前必须重新满足的门禁

1. 中央后台：解决或由授权方正式裁决 Core failure/error；完成中央 Hermetic Build 与 immutable verify；净化或接受 49 个绝对路径；从白名单生成干净 staging tree 并复扫。
2. 共享 MCP：确认 36 个工作树变化的精确提交范围；形成用户授权的 commit；决定是否接受历史中的本机路径；创建并验证 Private 仓库后再配置 `origin`。
3. 数学、408、英语：分别在原根外生成 sanitized staging tree；只复制本报告精确白名单；修复绝对路径和 408 phone-like 命中；重新扫描；绝不复制原 `.git`，绝不修改原 Gitee remote。
4. GitHub：五个仓库创建后逐个验证 owner=`xialovezhu-wq`、visibility=`PRIVATE`、default branch=`main` 和预期 remote URL。
5. push 会话在执行前重新核对待提交对象、分支、remote、仓库可见性和最终 SHA；本报告不授权 commit、建仓、remote 修改或 push。

PUSH_READINESS_STATUS: PARTIAL
