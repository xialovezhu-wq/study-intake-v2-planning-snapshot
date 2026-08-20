# 学习预处理中心 Dashboard

这是 `study-intake-preprocessor` 的只读本机界面。它只读取 Worker 原子生成的投影，不读取权威账本、不调用模型，也不执行任何正式写入。

## Production Canary 运行态

v3 summary 可以携带每科独立、由 Dispatcher HMAC 状态绑定的
`canary_gate`。新的 live 状态只接受 v2 canary state/receipt 合同，且
`fast_mode_requested=false`、`fast_mode_effective=not_requested`。旧 v1
或旧 priority campaign 只可标为历史证据，不能作为当前 release 的运行
状态。successor 激活时必须一次形成三科 gate；部分形成、跨 release、
authority digest 不一致、正式写入非零或 Sol 被启用都会让投影失败关闭，
不能显示为局部成功。

公开 API 只输出无本地路径的运行字段：

- `producer_capture_enabled`、`luna_consumer_enabled`、
  `sol_formal_curation_enabled` 三个独立状态；
- canary 槽位的 `queue_depth`、`oldest_pending_age_seconds` 与旧兼容字段
  `active_task_count`；
- 真实 lease/queue/HMAC telemetry 闭包产生的
  `subject_active_task_count`、`subject_pending_task_count`、
  `global_active_task_count`；已核验的 task-process identity/exit 半开区间产生
  `subject_peak_active` 与 `global_peak_active`。调度认领高水位通过
  `scheduler_claim_subject_peak_active` / `scheduler_claim_global_peak_active`
  单独显示，不能冒充真实进程峰值；
- `verified_runner_active_*` 表示当前仍有有效 task-process identity 且未形成
  exit receipt 的 runner 数；`runner_evidenced_task_count_*` 与
  `runner_interval_missing_count_*` 显式给出生命周期证据覆盖。只要缺口非零，
  live active 数仍可显示，但实际峰值必须为 null、运行态降为 partial，health
  必须返回 503；
- 每科 HMAC terminal index 的 `terminal_task_count` 与
  `terminal_by_outcome` 必须闭合；失败 outcome 还必须逐项绑定无路径的
  `unit_sha256`、`terminal_receipt_sha256`、`terminal_kind` 和安全错误码。
  Dashboard 绝不公开 terminal index、receipt 或 report 的物理路径；
- `effective_concurrency_limit` 表示当前状态允许的并发上限，
  `available_concurrency_slots=max(0, effective-active)` 表示剩余可认领槽位；
  首条 canary 的 limit 为 1，持续并发解锁后使用 HMAC state 中的配置值，
  paused/failed/inactive 的新认领 limit 为 0；
- `effective_concurrency_limit=null` 只有同时标明 `unbounded` 才表示无固定
  上限；来源缺失、跨 release、遥测/lease/canary 不一致时同样返回 null，
  但必须标明 `unavailable`，禁止伪造 0；
- 当前 capture、阶段、逐科 timeout、最近成功/失败、阻塞原因和下一步；
- 分作用域的计数：canary gate 控制面 model/Provider/MCP 固定为 0；激活期
  observed 计数只标为 `current_activation_cumulative`；当前或最近 terminal
  任务的 model/Provider/MCP 计数必须重开权威 stage receipt 后才显示，receipt
  闭包缺失时返回 null，禁止用 gate 的 0 覆盖；
- report/package digest、release、Dispatcher release 与 component lock；
- `production_canary_active`、`runtime_canary_verified`、`paused`、`failed`
  状态，但绝不生成 `production accepted`。

`continuous_concurrent_unlocked` 是持续并发消费的 live 状态。首条 canary
仍然只能单 in-flight；解锁后的并发上限不再沿用首条 canary 的 1。若
`failed_drained` 时还有 siblings 正在收束，活动数可以暂时大于 0，但有效
新认领上限必须为 0。

全局活动数、终态累计与两类峰值只来自 Dispatcher 已验证的共享 HMAC
mutable telemetry。实际峰值使用 task-process identity 的 `launched_at` 到
exit receipt 的 `finished_at` 半开区间，并规定同一时间点先处理 end、再处理
start；零时长区间不计作重叠。Dashboard 不从历史页面值补算峰值，不把
scheduler claim 高水位标为实际并发，也不把 mutable telemetry 当作最终三科
并发验收；最终验收仍必须绑定独立、不可变的完整生命周期证据。

并发验收面板按 schema 分流：v2 只读取 `model_request_contract` 与
`concurrency`，以 terminal/active/lifecycle 闭包和真实进程峰值展示当前证据；
不会回退读取 v1 的 `barrier` 或 `fast_mode`。v1 的 barrier/priority 信息只在
`historical_legacy` 视图显示，即使 release ID 恰好与当前 release 相同，也必须
保持 `current_release_usable=false`。

v2 每科只公开受控的 `provider_execution_contract_status`、Analysis/Critical
Review 两阶段 provider identity/exit SHA-256，以及 provider/runner executable
SHA-256；不公开 argv、环境变量名、可执行文件路径、cwd 或 receipt 物理路径。
真实结果只有在状态为 `verified_no_fast_mode_argv_environment`、两阶段四个
identity/exit hash 均闭合、Analysis 与 Critical Review 完成，并且 JSON、
Markdown 与 package 均通过 reopen 后才能显示为 verified。零模型 fixture 必须
标为 `zero_model_fixture_not_applicable`；任一缺口统一显示 `not_verified` 或失败
关闭，不能从模型请求合同的自报字段推断 Provider 已核验。

Canary 只接收 activation high-watermark 之后的 producer record。旧
backlog 保持冻结。单科失败时，该科 producer 与持久队列继续工作，
只暂停该科 Luna consumer；另外两科保持独立。

## 固定入口

- 地址：`http://127.0.0.1:8767/`
- 监听：仅 `127.0.0.1:8767`
- 默认投影：`~/.codex/study-intake-preprocessor/state/dashboard_projection.json`
- 日期归档：`~/.codex/study-intake-preprocessor/state/dashboard-projections/YYYY-MM-DD.json`
- 三科并发验收投影：`~/.codex/study-intake-preprocessor/state/three-subject-concurrency-campaign.json`
- 启动：`python3 ~/.codex/study-intake-preprocessor/dashboard/server.py`

测试时可以用 `STUDY_PREPROCESSOR_DASHBOARD_PROJECTION` 指向临时投影；监听地址和端口不可由环境变量覆盖。

## 投影合同

三个 Dispatcher 的投影汇聚器必须通过“写入同目录临时文件 → flush/fsync → 原子 replace”的方式发布以下 schema：

```json
{
  "schema_version": "study-intake-dashboard-projection-v3",
  "generated_at": "RFC3339 时间",
  "study_date": "YYYY-MM-DD",
  "dispatchers": {
    "math": {
      "status": "running",
      "last_heartbeat": "RFC3339 时间",
      "release_id": "64 位 release id",
      "requested_model": "gpt-5.6-luna",
      "requested_reasoning_effort": "max",
      "current_stage": "analysis",
      "paused": false
    },
    "cs408": {"同结构": true},
    "english": {"同结构": true}
  },
  "subjects": {
    "math": {
      "enabled": true,
      "status": "active",
      "current_stage": "analysis",
      "counts": {
        "selected": 1,
        "queued": 0,
        "analysis_running": 1,
        "critical_review_running": 0,
        "terminal": 0,
        "quality_passed": 0,
        "needs_rework": 0,
        "failed": 0,
        "evidence_pending": 0
      },
      "batch_state_available": false,
      "batch_id": null,
      "batch_status": "no_data",
      "capture_high_watermark": null,
      "authority_generation": null,
      "all_terminal": null,
      "sol_ready": null,
      "blockers": [],
      "sol_handoff_status": "unknown",
      "pipeline": {},
      "items": [],
      "metrics": {},
      "metric_sources": {}
    },
    "cs408": {"同结构": true},
    "english": {"同结构": true}
  },
  "global_sol": {
    "state_available": false,
    "status": "no_data",
    "active_subject": null,
    "active_batch_id": null,
    "current_item": null,
    "authorized_queue": [],
    "fencing_token": null,
    "active_writer_count": null,
    "committed_count": null,
    "remaining_count": null,
    "formal_write_count": null,
    "updated_at": null
  }
}
```

v3 要求数学、408、英语三个学科、三个 Dispatcher 和全局 Sol 写入面同时存在。`selected` 必须由排队、两个运行阶段、终态和待证据五类互斥计数闭合；`terminal` 必须由 `quality_passed`、`needs_rework`、`failed` 三类闭合。服务端根据公开 item 再计算一次，不把生产者预聚合值当作第二份事实。

`all_terminal=true` 只表示冻结任务全部结束，可以包含失败；`sol_ready=true` 还要求非空冻结批次全部质量通过、package 和质量 receipt 完整且 blocker 为空。批次状态文件缺失时两者必须为 null，页面显示未形成状态，禁止显示 0/0 完成。

`global_sol` 只来自全局 writer lease 的安全只读投影。不存在时所有计数保持 null；存在时 `active_writer_count` 只能为 0 或 1，授权队列按授权 receipt 时间 FIFO 展示。`sol_reviewed` 与 `sol_committed` 分账，复核不能冒充正式写入。

三个 Dispatcher 分别公开心跳、release id、当前阶段，以及固定请求身份 `gpt-5.6-luna` / `max`。`healthz` 逐个核验启用学科，不允许一个 watcher 或一个学科的心跳代表整个系统健康。

每个 Dispatcher 只负责生成本学科当天全部任务的安全投影。共享汇聚器在同一个短文件锁内更新学科投影，并重新生成主 v3 投影；它只读取安全 decision、task-event 索引、轻量 HMAC 事件和控制面安全状态，不打开 prompt、图片或答案正文。某学科冻结或读取失败时，其他学科仍独立前进；当天任务不按固定条数截断，跨日历史由日期归档承担。

列表中的开始时间、最后状态变化时间、本地模型请求是否已提交和已运行秒数来自 HMAC 阶段事件；终态耗时在最后状态变化处封顶，不随投影刷新继续增长。只有真实 `retry_wait` 服务限流事件才显示 `rate_limited / confirmed_event`，其余 server queue 一律保持 `unknown / unconfirmed`。规则版本优先展示绑定请求模型、推理档位、学科处理合同与 release 的内容寻址 SHA-256。

408 Worker 只接受同时满足以下条件的事实记录：`quality_status=awaiting_daily_curation`、行与 capture 均已授权、尚未进入活动批次、日期一致且 payload SHA-256 有效。`captured_unconfirmed`、`saved_neutral`、`curation_failed`、`needs_user`、`curating`、`curated` 和 `already_current` 均不进入 Luna 队列。408 capture 一旦完成耐久事务，可在下一次 Worker 轮询直接预处理；它不依赖数学的 90 秒防抖。

两个学科的状态读取互相隔离。某一学科状态源失败时，另一学科继续处理；失败学科投影为 `status: source_error` 并只公开安全错误代码。

### 每题公开字段

| 字段 | 约束 |
|---|---|
| `capture_id` | 必填，稳定 ID，只允许字母、数字、点、下划线、冒号和短横线 |
| `subject` | `math` 或 `cs408` |
| `study_date` | `YYYY-MM-DD` |
| `captured_at`、`updated_at` | RFC3339 时间 |
| `target_id`、`target_label` | 答案安全的题目标识 |
| `queue_state` | 七个互斥 Dashboard 状态之一：`queued`、`running`、`ready`、`needs_rework`、`failed`、`stale`、`evidence_pending`；`stale` 在 v3 终态计数中归入 failed |
| `current_stage` | 当前处理阶段；用于直接展示进度，不由计数猜测 |
| `luna_status` | 处理细节状态；保留 `queued`、`processing`、`retrying`、`ready`、`two_pass_ready`、`single_pass_degraded`、`failed`、`stale`、`skipped` 等细粒度回执 |
| `duration_seconds`、`retry_count` | 非敏感运行数据 |
| 内容摘要 | 默认列表和默认详情不接受生产者自报的自然语言摘要；只显示状态、哈希和字段级差异，答案性结构化内容仅在本机主动展开后读取 |
| `package_version`、`package_fingerprint`、`input_fingerprint`、`evidence_hash_status` | 可公开校验信息 |
| `pipeline_status`、`two_stage_status`、`quality_gate_status` | 双轮处理与质量门禁状态；`single_pass_degraded` 不能作为可交付包 |
| `requested_model`、`requested_reasoning_effort` | 模型请求身份；不能单独证明实际运行身份 |
| `runtime_identity_status`、`runtime_model`、`runtime_reasoning_effort` | 实际运行身份回执；只有三者完整且状态为 `confirmed` 才显示为已确认 |
| `evidence_claim_count`、`evidence_claims_with_refs`、`evidence_coverage_pct` | 有明确分母的证据引用覆盖；缺字段时显示“未记录”，不推断为零 |
| `visual_evidence_required`、`visual_claim_count`、`visual_claims_with_refs`、`visual_coverage_pct` | 视觉证据要求和覆盖；仅统计投影显式提供的质量回执 |
| `processing_contract_sha256`、`quality_receipt_sha256`、`evidence_manifest_sha256` | 处理合同、质量门禁和证据清单哈希 |
| `stage_receipts` | 首轮分析与独立批判复核的安全回执元数据；只公开状态、模型身份、耗时和哈希，不公开 prompt、答案或本机路径 |
| `last_error_code`、`retry_at_hint`、`critical_resume_status` | 公开安全的失败代码、重试时间提示和批判复核恢复状态；拒绝路径与控制字符 |
| `delivery_status` | `offered` 或 `consumed`，只表示已提供或已读取 |
| `adoption_status` | `direct_adopted`、`minor_edit_adopted`、`major_edit_adopted`、`modified_adopted`、`rejected`、`fallback`、`unknown` |
| `adoption_bucket` | Dashboard 派生的互斥桶：`direct`、`minor`、`major`、`legacy_modified`、`rejected`、`fallback`、`unknown` |
| `adoption_receipt_id`、`adoption_recorded_at` | 真实 Sol 采用回执；没有 receipt 时采用状态和桶一律按 unknown 展示 |
| `obsidian_url` | 可选，只接受 `obsidian://` 或 `http://127.0.0.1:8765/` |

`offered` 和 `consumed` 不能推导采用率。Dashboard 只有在 `adoption_status` 为非 unknown 且存在 `adoption_receipt_id` 时，才计入真实采用回执。旧 `modified_adopted` 只进入 `legacy_modified`，不会被猜测为小改或大改。

### 可观测性汇总

`GET /api/v1/summary` 根据公开逐题记录重新计算 `observability`：

- `processed`、`remaining`：已经进入终态的题数与仍待处理题数。
- `two_pass`：双轮通过和单轮降级数量。
- `runtime`：请求模型、逐题运行身份分布及已确认的实际模型组合。
- `evidence`、`visual`：仅基于显式 claim 分母的加权覆盖率；没有回执时不显示虚构的 0%。
- `adoption`：直接、小改、大改、旧版修改、未采用、兜底和未知七个互斥桶。
- `receipts`：采用、阶段运行和质量回执覆盖题数。
- `failures`：失败总数及安全错误代码分布。

### 后台任务详情

列表投影只携带 `unit_sha256`、输入指纹、冻结 payload 哈希、release id、rule version、`server_queue_status` 和 task-detail 定位信息。`GET /api/v1/items` 不打开 task-detail、事件、completion、package、receipt、prompt、结果正文或图片文件。

只有请求单条 `GET /api/v1/items/{capture_id}` 时，服务才按需读取：

- `dispatch/state/task-details/<unit_sha256>.json` 的 HMAC 封装详情；运行时提供 verifier 时标为 `hmac_verified`，否则只标为 `sealed_unverified`，不伪装成已核验。
- `dispatch/state/task-events/<unit_sha256>/fence-<n>/` 的 append-only 阶段变化索引，以及对应的 content-addressed 安全事件对象。心跳不是阶段事件，不进入时间线。
- standalone Dispatcher verifier 确认的 authoritative completion。completion 的 unit、release、fence、subject 或 capture 不一致时返回 `409 task_detail_generation_conflict`，旧代结果不能覆盖当前详情。

详情公开任务身份、attempt、fence、输入哈希、rule version、证据完整性声明、分析/批判复核阶段、检查点哈希、最终 package/receipt 哈希和安全错误代码。权威完成态只在详情请求中打开一次对应 package；页面只显示受保护字段名、摘要哈希、完整叶节点数量和不静默截断的递归确定性变更字段，不返回 prompt、答案或完整模型输出。408 图片只从冻结任务与 HMAC readiness 中汇总角色数量、顺序和验证状态，不展开图片或路径。`server_queue_status=unknown` 一律显示“未确认”；不由本机进程或文件存在性推断 server queue。缺少可验证字段时页面明确显示“未确认”，degraded completion 明确不可消费。

默认详情不会返回完整结构化输出。只有本机用户主动点击后，浏览器才以一次 `raw=1` 请求读取 HMAC 权威 package 中的 analysis、critical review 和最终结构化结果，并用折叠的 `<details>` 本地保留；这些是结构化输出，不是隐藏思维链。定时详情刷新永远使用 `raw=0`，运行中每 5 秒只刷新当前题，等待态降为 30 秒，终态停止；列表关闭详情后才恢复每 30 秒刷新，页面隐藏时全部暂停。prompt、冻结任务私有输入和图片内容不会进入该响应。详情读取和反复刷新固定 `model_call_count=0`、`formal_write_count=0`。

严禁把以下内容写入投影：原始 prompt、模型完整响应、完整题干、选项、最终答案、完整解析、凭据、绝对路径、临时文件位置和任意文件内容。即使误写，HTTP 服务也只返回字段白名单。

### 有来源的收益指标

`metrics` 中的数值只有在同名 `metric_sources` 提供非路径证据 ID 时才会进入 API。例如：

```json
{
  "metrics": {"review_time_delta_pct": -25.0},
  "metric_sources": {"review_time_delta_pct": "eval-receipt:trial-001"}
}
```

页面可以直接从真实 item 回执计算就绪率、延迟、失败/过期和采用回执覆盖。节省时间与质量提升若没有带来源的对照指标，会明确显示“尚未形成证据”。

## GET-only API

- `GET /healthz`
- `GET /api/v1/summary?date=YYYY-MM-DD&subject=all|math|cs408|english`
- `GET /api/v1/concurrency-campaign?subject=all|math|cs408|english`
- `GET /api/v1/items?date=YYYY-MM-DD&subject=all|math|cs408|english&status=...`
- `GET /api/v1/items/{capture_id}?date=YYYY-MM-DD&subject=...`

所有非 GET 方法返回 `405`。服务校验 Host，发送 CSP、no-store、no-CORS、防嵌入和同源隔离响应头，静态文件采用固定路由而不是用户可控文件路径。

并发验收投影与日常队列投影分离。当前 v2 入口按科目异步发布首条 canary 状态，不要求三科 barrier，也不要求三科同时产生 capture。每科证据从 HMAC terminal index、terminal receipt、task-supervisor identity/exit、processing receipt、MCP grounding 和内容寻址产物重新打开；mutable telemetry 只在独立重算半开执行区间和峰值之后作为非权威交叉核对。只有真实执行区间重叠时才展示 overlap，不从 dispatcher 就绪状态推导并发。

当前 live v2 release 必须显式记录 `requested_service_tier=null`、`fast_mode_requested=false`、`fast_mode_effective=not_requested`，初始单科 in-flight 上限为 1，独立解锁后的持续并发上限为 20。入口公开 campaign/release、逐科状态、canonical MCP 调用数、Analysis/Critical Review、report/package 内容寻址绑定、真实重算峰值、Provider/模型计数、`formal_write_count=0` 和 `sol_enabled=false`。真实成功还必须显示受控的 `provider_execution_contract_status=verified_no_fast_mode_argv_environment`，Analysis 与 Critical Review 各自的 Provider identity/exit SHA，以及 Provider 和同 release task runner 的 executable SHA。Dashboard 不序列化 argv、环境变量键名、可执行文件路径或 receipt 本地路径。只有 terminal、processing、package 和独立 report 闭包均可重开时，逐科状态才允许进入 `verified`；缺少 report 绑定的 package-only 结果保持部分证据，不得使 campaign 进入 `production_verified`。

零模型 fixture 必须使用 `zero_model_fixture_v2`，实际模型、Provider 和 MCP 计数保持 0，Provider 执行合同固定为 `zero_model_fixture_not_applicable`，并固定为 `test_evidence_only`、`current_release_usable=false`。旧 v1 priority campaign 只能标为历史，不可作为当前 release 证据。缺失时返回 `available=false`；损坏时返回 503，且安全计数为 null，禁止把不可用状态伪装成零调用证明。任何 campaign 的 `production_accepted` 都固定为 false。

请求日期与当前投影日期不一致时，服务只读取同目录的日期归档；归档不存在则返回 `available: false` 和 `projection_date_unavailable`，不会伪装成 `available: true` 的空列表。投影不存在时 API 返回 `available: false` 和 `projection_missing`；投影损坏或 schema 不匹配时返回真实错误与 `503`。`healthz` 分开公开 `http_ready`、`projection_fresh`、`luna_processing_ready` 和 `sol_writer_ready`。任一启用 Dispatcher 心跳超时、暂停、非运行、旧代 heartbeat 或 release 不一致时，旧心跳不参与 readiness。只有活动 Sol 状态与另外两科新鲜 heartbeat 同时成立时，页面才确认“其他学科 Luna 继续运行”。

## 测试

测试 fixture 仅用于自动化验证，不会被运行时读取：

`~/.codex/study-intake-preprocessor/dashboard/tests/fixtures/dashboard_projection.json`

运行：

```bash
python3 -m unittest discover -s ~/.codex/study-intake-preprocessor/dashboard/tests -v
```
