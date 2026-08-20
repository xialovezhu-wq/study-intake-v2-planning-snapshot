# Study Intake Multi-Agent V2 Worklog

## 2026-08-17T13:56:16+08:00 阶段 0 进行中

- 执行包 SHA-256：`8f6d562bc1272a60a19100a2716ea25b9eb98c6b9c929d16fb46631087b1ffee`。
- 执行包外层 `SHA256SUMS` 全部通过。
- 技术说明包 `evidence/SHA256SUMS` 全部通过。
- 已完整读取 1459 行最终实施规格、说明包 README、8 份 docs 和 4 份 evidence 摘要。
- 已对参考 source 完成全文件清单、行数、SHA 和关键合同结构索引；正在对真实 source of truth 做定义/调用/模型假设全量审核。
- 已定位真实可写 authoring 源树：`/Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor`。参考包所含中央源文件与该源树及当前 immutable release 的 SHA 逐文件一致。
- 当前 release：`aa59e98b974b64144933a87b42eb92120b9eb3d8145469829b2ab89911104fcb`。
- 现场发现旧部署三科 consumer 仍为 armed/enabled，与本轮禁止 live scan/claim/consume 边界冲突。
- 已使用现有精确 `pause-canary` 合同将 Math/CS408/English 转为 `paused_drained`，确认 active task 和 queue 均为 0；随后精确 bootout 三个 Dispatcher LaunchAgent。
- Dashboard 保留运行；因 Dispatcher 主动停止，`/healthz` 正确降级为 503/degraded。
- 现场计数：Terra 0，Luna 0，Provider 0，production MCP 0，live capture consumed 0，formal write 0，`production_accepted=false`。

## 未完成

- 完成真实源树与三科 Producer/Skill/Schema/fixture 的全量只读基线审核。
- 封存 formal surface 与三科 Capture 历史基线。
- 写入 `BASELINE_AUDIT.md` 与 `IMPLEMENTATION_PLAN.md`。
- 实施、测试、Build、verify、preflight、transactional deploy 和最终离线验收。

## 下一条精确命令

```sh
rg -n -i 'gpt-5\.6-luna|model_call_count.{0,16}2|agents\.enabled|agents_enabled|one.*mcp|sol_ready|quality_pass|critical_review' /Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor
```

## 2026-08-17T14:50:34+08:00 阶段 2–7 实施检查点

- 新增 offline/live gate、manual one-shot authorization/relock、Terra/Luna/reviewer 角色合同、read plan、multi-agent event parser、branch worker、动态分波调度、跨 task round-robin fairness、read bundle、risk/review/Sol handoff 和 Host runtime。
- 新增 17 份 V2 Schema，全部顶层字段闭包，并纳入 staging generator 和 component lock。
- 固化 release-bundled `multi-agent-read-orchestrate` Skill、Terra orchestrator、Luna reader、fresh Terra reviewer 配置和三份 tool policy。
- config 默认 `execution_mode=offline`，live gate locked，逻辑 branch limit=null，物理并发 dynamic/queue-in-waves/drop-never。
- Dispatcher daemon 在任何 Producer scan 前进入 control-only heartbeat；`run-once`、`audit`、Task Runner 和 CodexRunner 均有代码级禁止门。
- 三科 Producer 现在对 high-watermark 之后新 Capture 写入 release-neutral sidecar attestation；历史 Capture 不回填。
- 三科 foreground Skill 已与新 receipt gate 对齐；408 authoritative/installed 副本 byte-identical。
- Scanner 对新 Capture 在模型前验证 descriptor、Skill parity、Producer closure、Capture contract、sidecar 和 component lock，并将 `processing_binding_v3` 纳入 immutable task identity。
- component lock 已重新生成：`8a9069a8fd5c2c3ee55d9ac24907695eff5003c7a5b67959d95c0d0d321f7ac9`。
- Dashboard 新增 GET-only `/api/v1/multi-agent-v2`，healthz 公开 offline/live-lock/零调用轴，前端标签升级为 Terra × Luna。
- Dashboard 完整 discovery：111 tests passed，0 failed，0 skipped。
- Multi-Agent/foreground binding/offline gate 定向：已通过 20+ tests；包含 3 分支真墙钟并发、8/3 分波、20/4 不截断、跨 task fairness、failure/cancel/timeout 回收、events 8 children、issues/quarantine 和 Popen 前 tripwire。
- 三科 Producer 全套定向在修改后通过：Math 60+1，CS408 46+1，English 42+1。
- Processing Plugin 回归：32 tests passed。
- 现场真实 sidecar 目录仍不存在；本轮 live Capture created/consumed 仍为 0。

## 当前未完成

- 完成剩余 Core/release-manager 定向回归和完整离线 discovery。
- 冻结新 fixture，运行 final release gate，Build/verify/preflight/deploy。
- 部署后四服务、8767 owner、healthz、projection、formal/Capture 不变验收。
- 最终报告与 manual smoke runbook。

## 新的下一条精确命令

```sh
STUDY_INTAKE_FIXTURE_EXECUTION=1 PYTHONDONTWRITEBYTECODE=1 /usr/local/bin/python3 -B -m unittest -v tests.test_backend_schema_contracts tests.test_backend_successor_contracts tests.test_processing_plugin tests.test_task_runner_events tests.test_subject_quality_receipt_v2 tests.test_successor_schema_roundtrip tests.test_production_canary_schema_contract
```

## 2026-08-17T15:49:53+08:00 阶段 8 离线回归完成

- 新 fixture：`e5b0a0558e7a1f92f41d9f655c1bb96c17e3ab6db81aae0d58298992740597df.json`；1253 个冻结文件、244 个 external verification 行；独立 verify 通过。
- Core 最终完整 discovery：1133 tests passed，0 failed，0 errors，0 skipped，766.270 秒。
- Dashboard 完整 discovery：111 tests passed。
- 三科 Producer 回归：Math 61、CS408 47、English 43，全部通过。
- Multi-Agent 定向证据通过：真墙钟并发、8/3 分波、20/4 无截断、跨 task fairness、branch 独立 PID/PGID/session、取消/超时回收、issues_found 与 quarantine 保留、真实 Codex 启动前 tripwire。
- 进程生命周期修复在完整负载下通过：使用 PID、PGID、稳定 OS start token 和当前 executable command 校验，没有扩大 signal scope。
- formal surface 构建前复核：`status=unchanged`，baseline/current manifest 均为 `cab25cf58a88ae31e5a8b1f2ae0d456350ea4a655a96e4ed11b5eaa70e78c352`。
- Capture history 构建前复核：manifest 仍为 `2d2ced7d199767e7fca4e5f1083087816b025da6105d9be15f5bfe0414262b52`；六个 root 的 content SHA 与基线逐项一致。
- Math、CS408、English 三个真实 producer-binding sidecar 目录仍不存在。
- 真实 Terra/Luna/Provider/production MCP/live Capture/formal write 计数均为 0；`production_accepted=false`。

## 当前未完成

- Hermetic Build、release verify、read-only preflight、transactional deploy。
- 部署后四服务身份、PID/PGID、WorkingDirectory、8767 owner、heartbeat、projection、healthz、live gate 与数据不变复核。
- 最终实施报告、离线验收报告、manual live validation runbook 与变更 SHA 清单。

## 下一条精确命令

```sh
cd /Users/xiazhibin/Documents/Codex/2026-08-10/lunamax-mcp-lunamax-lunamax-provider-codex/work/three-subject-successor && python3 -B scripts/release_manager.py build --help
```

## 2026-08-17T16:06:52+08:00 阶段 9 Build、部署与离线验收完成

- canonical immutable release：`ad1807186ac277c6bacfb7fd8d83b9cbc86cf75027698e70f994f3228d815cc9`。
- release manifest SHA：`b6e1eb335fd44a198eb7d8ad1bf04ba5c9bed93fe7bc43eb43847b321c10fcff`。
- Build gate：Core 138、Dashboard 111、skipped 0、real model 0、formal write 0；独立 verify 通过。
- 初次 `--release-base` 多传了一层 `releases`，得到未部署只读重复树；没有删除、没有切 current。随后使用标准 base 重建并 verify canonical 路径，release ID 不变。
- read-only canary preflight：`preview_non_mutation_verified=true`，四服务 planned running，三科 active/claimed 0，验证模型/Provider/MCP/formal 0。
- canary manifest：`f783f3c8feb20007a52bf184ea5e2aba2bc5de1a77388c3a0918d4bed63b3efe`。
- drain receipt：`ef03c5cd3087c8cd4237b9643f8efe96bf9c270e4ed45a46e8f7983f8dd609b4`。
- deployment prepare receipt：`0779a534ea3fb07b02f96a6fb200e2f4d4d3a0b902f7af797db0f2679cdef851`。
- canary activation receipt：`96bbbfb68fa8cabeb6c8b91fcb30e8fd5958bc4ef3c1ab41bdc531faa8b0eabd`。
- deployment postcommit receipt：`4451fa8ee9fbab36d3f7ee5e4296ef3e3ab2ab995c1b94adaaff4adf963ee15d`，status committed。
- current 已由 `aa59e98b...104fcb` 切到 `ad180718...15cc9`。
- 四服务 running：Math 16477、CS408 16481、English 16485、Dashboard 16489；四者 PID=PGID，WorkingDirectory=current，runs=1，未退出。
- 三个 Dispatcher 显式 `STUDY_INTAKE_EXECUTION_MODE=offline`，无 child process。
- 8767 唯一 owner 为 Dashboard PID 16489；healthz HTTP 200/ok，projection fresh。
- Multi-Agent API：offline、live gate locked、manual authorization absent、active/pending task 0、production accepted false。
- postdeploy formal manifest 仍为 `cab25cf58a88ae31e5a8b1f2ae0d456350ea4a655a96e4ed11b5eaa70e78c352`，差异为空。
- postdeploy Capture manifest 仍为 `2d2ced7d199767e7fca4e5f1083087816b025da6105d9be15f5bfe0414262b52`；三科 sidecar 目录仍不存在。
- 最终安全计数：Terra 0、Luna 0、Provider 0、production MCP 0、live Capture created/consumed 0、formal write 0。
- 已生成 `FINAL_IMPLEMENTATION_REPORT.md`、`OFFLINE_ACCEPTANCE_REPORT.md`、`PENDING_MANUAL_LIVE_VALIDATION.md` 和 `CHANGE_SHA256_MANIFEST.json`。

## 最终状态

```text
deployment_ready = true
immutable_release_deployed = true
offline_acceptance_passed = true
live_model_execution = false
manual_live_authorization_required = true
capture_smoke_pending = true
production_accepted = false
formal_write_count = 0
```

## 下一条精确命令

没有剩余的本轮自动实施动作。若只需重新验证 immutable closure，运行：

```sh
python3 -B /Users/xiazhibin/.codex/study-intake-preprocessor/current/scripts/release_manager.py verify --release-dir /Users/xiazhibin/.codex/study-intake-preprocessor/current
```

以后 live smoke 必须先阅读 `PENDING_MANUAL_LIVE_VALIDATION.md` 并由用户显式授权；不得自动执行。
