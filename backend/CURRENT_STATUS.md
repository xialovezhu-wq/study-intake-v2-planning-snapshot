# Study Intake V2 Current Status

更新时间：2026-08-20 14:00 +08:00

## 当前判定

`LOCAL_CLOSEOUT_STATUS: PARTIAL`

Schema byte-freeze 回归已经按版本化方式修复，三科最小验证和所有受影响定向测试通过。完整 Core 在 post-fix 运行中仍出现既有 Provider hard-cancel 生命周期竞态，因此中央 Hermetic Build 和 immutable verify 按门禁未运行。本轮没有 deploy、Git 暂存、提交、remote 修改或 push。

## 已完成

- 恢复三份历史 Schema 的原始 bytes，固定 SHA 与历史基线完全一致。
- 新增 `mcp-authority-snapshot-v2.json`、`mcp-authority-snapshot-receipt-v2.json` 和 `mcp-read-session-v4.json`。
- 共享 MCP 升级到 `0.4.1`，当前 active snapshot/session 合同迁移到 v2/v4；v1/v3 仅作为历史兼容保留。
- 构建并验证共享 MCP 依赖 release `21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425`。该动作没有安装、切换 current 或 deploy。
- Host、Plugin、component lock、生成器、background Skills、引用合同和测试已迁移到 successor。
- 三科 canonical/installed Skill parity、authoring binding closure 和每科一个 smoke 均通过。
- 六处 `.gitignore` 已逐文件复核；协调根和 408 的过宽规则已经收窄。
- 已完成当前 Git 历史的文件名和大 blob 风险扫描，没有读取或输出秘密内容。

## 已测试

| 范围 | 结果 |
|---|---|
| 历史 Schema byte-freeze | PASS，1 test |
| successor Schema compatibility | PASS，1 test |
| successor inventory/roundtrip | PASS，14 tests |
| Host、Plugin、MCP binding | PASS，91 tests |
| foreground binding 与 Sol MCP preflight | PASS，12 tests |
| shared MCP affected tests | PASS，22 tests |
| shared MCP full | PASS，74 tests |
| final component generator check | PASS |
| final three-subject authoring alignment | PASS，3/3，receipt `3c468350…b2f8` |
| Math smoke | PASS，1 test |
| CS408 smoke | PASS，1 test |
| English smoke | PASS，1 test |

## 完整 Core

首次阶段 B 运行：1144 tests，797.243 秒，3 failures。两项为当前 successor contract 文档 SHA 迁移点，随后已修复；一项为 Provider grandchild 启动等待超时。

最终 post-fix 运行：1144 tests，786.745 秒，1 failure、1 error，均来自 `test_direct_supervisor_sigterm_reaps_provider_grandchild`：退出索引竞态和临时目录仍有晚到事件。该模块在定向运行中通过，且修改面与 Schema successor 无交集，因此分类为 `HISTORICAL_FAILURE`，本轮不扩大到 Provider 生命周期重构。

日志：

- `evidence/prelive-finalization-20260818T162612+0800/logs/core-stage-b-20260820.log`
- `evidence/prelive-finalization-20260818T162612+0800/logs/core-stage-b-postfix-20260820.log`

## 未测试或未执行

- 中央 Hermetic Build：`NOT_RUN`，因完整 Core 未达到零失败。
- 中央 immutable verify：`NOT_RUN`，依赖 Build。
- deployment、current release switch、deployed Validation Console E2E：本阶段禁止，均未执行。
- Git add、commit、git init、GitHub 仓库创建、remote 修改、push：均未执行。

## 三科学科仓库边界

数学、408、英语在本轮保持 `VERIFICATION_ONLY`。三科非 `.gitignore` 的 `git status --porcelain` 指纹在验证前后完全一致，没有整理或改写其历史工作区。

## `.gitignore` 决定

| 文件 | 决定 | 说明 |
|---|---|---|
| 协调根 `.gitignore` | KEEP，已收窄 | 撤回整个 `evidence/` 和通配 `output/`；仅忽略 evidence 临时日志、private MCP transcript、缓存、秘密类型和解压包 |
| 中央后台 `.gitignore` | KEEP | 仅保护 runtime dirs、缓存、环境文件、数据库、备份、构建物和可重建 freeze marker；不忽略源码、Skill、fixture、tests 或状态文档 |
| 共享 MCP `.gitignore` | KEEP | 仅缓存、虚拟环境、环境文件、数据库、备份和构建物 |
| 数学 `.gitignore` | KEEP | 保护事务锁、个人快速入库来源/账本、错题卡、生成物、缓存和秘密类型；canonical Skill 与 tests 未被忽略 |
| 408 `.gitignore` | KEEP，已收窄 | `.codex-*` 收窄为 evening-redo；整个 benchmarks 收窄为两个已知生成目录；个人 state、题目和复习卡继续排除 |
| 英语 `.gitignore` | KEEP | 保护 intake、tmp、raw、articles、wiki、mastered 状态、缓存和秘密类型；pipeline、Skill、schema 与 tests 未被忽略 |

## Candidate commit paths

中央后台未来新仓库候选：

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

共享 MCP 候选：

- `.gitignore`
- `README.md`
- `config/`
- `pyproject.toml`
- `requirements.lock`
- `scripts/`
- `src/`
- `tests/`

三科仅允许后续按 component descriptor 白名单选择代码、Schema、canonical Skill、直接测试和本轮 `.gitignore`；不得 blanket-add 整个脏工作区。

## Excluded paths

- 中央：`config.json`、`release.json`、`logs/`、`state/`、`packages/`、`private/`、`receipts/`、缓存、venv、build/dist、freeze marker。
- 共享 MCP：`.venv/`、缓存、环境文件、数据库、build/dist；immutable release 位于 repo 外，也不提交。
- 数学：PDF、快速入库来源和账本、错题卡、生成目录、`.codex_tmp/` 及未列入白名单的个人 wiki/图像。
- 408：`wiki/study_vaults/408-full/state/`、key、SQLite、题目和复习卡运行数据、dated dashboard queues、benchmark run output。
- 英语：`intake/`、`tmp/`、`raw/`、`articles/`、`wiki/`、deferred intake、mastered 状态和个人 bank 变更。
- 协调根：测试日志、private MCP transcript、解压执行包、ZIP 和未明确列入 handoff 白名单的旧报告/生成物。

## Repository mapping proposal

- 中央后台：未来在当前 authoring tree 建立独立私有仓库 `study-intake-v2-backend`；因无既存 remote，GitHub 使用 `origin`。
- 共享 MCP：映射为私有仓库 `local-study-read-mcp`；当前无 remote，GitHub 使用 `origin`。
- 数学、408、英语：保留现有 Gitee `origin`，未来 GitHub 使用新 remote 名 `github`，不得覆盖 origin。
- 协调与证据目录：最低基线建议继续本地保存，不作为必需 GitHub 仓库。若以后确需版本化，只能用白名单建立独立私有 `study-intake-v2-handoff`，不得复制完整 evidence/private/package 树。
- 不创建 monorepo 或 submodule。

## 敏感历史风险

- 408 Git 历史对象路径中发现 18 个 `.key` 和 26 个 SQLite 路径。
- English Git 历史对象路径中发现 4 个 SQLite、2 个 WAL、2 个 SHM 路径。
- Math Git 历史含一个约 19.4 MB 的生成 JSON blob。
- 共享 MCP 历史文件名扫描未发现上述类型。

因此，任何把既有 Gitee 历史镜像到 GitHub 的动作前，必须执行完整历史 secret/database 扫描并决定是否需要密钥轮换、历史清理或只迁移经审计的新分支。本轮没有改写历史。

## 下一步

等待用户审核。若要继续达到 `READY_FOR_PUSH_REVIEW`，需另行授权一个窄范围 Provider hard-cancel race 修复阶段；修复后重新运行完整 Core，只有零失败才可执行中央 Hermetic Build 和 immutable verify。
