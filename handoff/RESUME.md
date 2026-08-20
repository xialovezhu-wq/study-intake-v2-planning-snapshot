# Study Intake V2 Local Closeout Resume

## 当前阶段

2026-08-20 阶段 B 已完成 Schema successor 修复、定向测试、三科最小验证、`.gitignore` 裁决、状态文件和仓库映射建议。最终状态为 `PARTIAL`。

## 当前阻塞

完整 Core post-fix 结果为 1144 tests、1 failure、1 error。失败集中在 Provider hard-cancel 生命周期竞态：supervisor 退出索引尚未可见，以及临时目录删除时仍有晚到事件。该问题不在本轮 Schema successor 直接修改范围内。

按用户门禁，中央 Hermetic Build 和 immutable verify 没有运行；不得将共享 MCP 依赖 release 的独立 build 误写成中央 Build 通过。

## 已完成的 successor 状态

- 历史 Schema v1/v3 bytes 已恢复并通过固定 SHA 测试。
- active successor 为：
  - `study-read-mcp-authority-snapshot.v2`
  - `mcp_authority_snapshot_receipt_v2`
  - `study-read-mcp-read-session.v4`
- 共享 MCP source version：`0.4.1`。
- 共享 MCP dependency release：`21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425`。
- 中央 component lock SHA：`6f4cae2908d228640695e2d762c722dc865fcc16b429045bed47e13e5cf2c848`。
- 三科 authoring alignment receipt：`3c46835069811f7d42fb577d7b37ebe11c588b92b978d9ec5bdd41254a0682f8`。

## 权威状态文件

- `/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor/CURRENT_STATUS.md`
- `/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor/WORKSPACE_MANIFEST.yaml`
- `/Users/xiazhibin/Documents/ChatGPT/LunarMax 后台处理的规划/WORKLOG.md`

## Core 证据

- `evidence/prelive-finalization-20260818T162612+0800/logs/core-stage-b-20260820.log`
- `evidence/prelive-finalization-20260818T162612+0800/logs/core-stage-b-postfix-20260820.log`

## 恢复规则

1. 先读后台 `CURRENT_STATUS.md` 和 `WORKSPACE_MANIFEST.yaml`。
2. 不改动数学、408、英语历史工作区；它们保持 verification-only。
3. 不自动进入 Build、deploy、Git commit 或 push。
4. 若用户授权修复当前阻塞，只处理 Provider hard-cancel race，并先建立独立、可复现的 full-suite-load 失败测试。
5. 修复后重新运行完整 Core；只有零失败才能运行中央 Hermetic Build 和 immutable verify。
6. 既有 Gitee 历史 push 到 GitHub 前必须完成全历史 secret/database 扫描；不得直接镜像包含 key/SQLite/WAL/SHM 的历史。

## 下一步

等待用户审核阶段 B 报告。当前没有已授权的自动执行命令。
