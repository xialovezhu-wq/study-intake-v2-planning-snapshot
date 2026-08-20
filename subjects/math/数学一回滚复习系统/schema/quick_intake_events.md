# 数学快速入库事件与夜间收口合同

## 目标

学习中的单题错题记录以“立即保存真实证据、尽快释放会话”为目标。高质量正式卡改写、全量 wrongnet 重建、定向回滚和 Wiki 收口统一移到夜间批次。

这个拆分不降低证据标准。它把不可变的学习事实与可优化的正式表示分开：

- `快速入库事件.jsonl` 保存用户当次实际作答、断点、提示与修正证据。
- 正式错题卡、回滚单元、wrongnet 生成层和 Wiki 是夜间根据这些事实更新的表示层。
- 夜间不得反向覆盖、删改或“润色”快速事件中的原始事实。

## 权威文件

- 快速事件账本：`数学一回滚复习系统/快速入库事件.jsonl`
- 新临时来源固化目录：`数学一回滚复习系统/快速入库来源/`
- 写入工具：`数学一回滚复习系统/scripts/quick_intake.py`
- 并发锁：`数学一回滚复习系统/.快速入库事件.jsonl.lock`
- 来源固化锁：`数学一回滚复习系统/.快速入库来源.lock`
- 正式收口锁：`数学一回滚复习系统/.正式层写入.lock`

锁文件不是学习数据，必须由 Git 忽略。账本采用追加式 JSONL；`pending` 状态由 capture、amendment、freeze、prepare、invalidation 与 closeout 回放得到，不原地修改旧事件。

## 白天快速路径

正常 warmup 错题按以下顺序处理：

1. 用已有 `scheduler.py score-warmup` 写入唯一队列项的评分事件。
2. 每个 fresh Capture 都先用 `quick_intake.py stage-source` 固化 source bundle v1；bundle 必须包含真实题图和独立解答文本 artifact。
3. 构造一个 `math-fast-intake-capture-v2` 证据包，保存解析文本、用户原始作答和相关教学对话。历史 v1 事件只读兼容，不重写。
4. 用 `quick_intake.py record` 追加 capture。
5. 收到 `state: pending_nightly` 后立即结束本次入库，让用户继续下一题。

快速路径只允许三类持久化仓库写入；`/private/tmp` 的一次性 payload 不属于学习事实：

- canonical warmup 评分。
- 每个 fresh Capture 对应的不可变来源包。
- 快速事件账本。

快速路径禁止：

- 改正式错题卡。
- 改 `复习单元.json`，warmup 评分自身的规范更新除外。
- 执行 `upsert-wrongnet`。
- 执行 `wrongnet.py rebuild`。
- 扫描关联旧卡或生成关系提案。
- 更新 Wiki。
- 读取完整历史任务或全库材料来追求即时“完美改写”。

端到端性能目标为正常路径 30 秒内，验收门槛为 P95 不超过 60 秒。fresh Capture 路径包含来源 payload、`stage-source`、capture payload 与 `record`，硬上限为 5 次工具调用，并最多进行一次确定性重试。writer 微基准只证明确定性写入开销；模型组织证据、现场评分、来源固化和 payload 创建必须另用端到端任务样本衡量，不能用 writer P95 冒充整条路径 P95。

## fresh Capture 来源固化

所有 fresh Capture 都必须在 capture 前固化 source bundle v1。正式卡复发也不能省略 bundle，且不使用正式卡答案作为解答文本 fallback。来源可以是本轮临时文件，或显式提供的真实仓库内证据：

- `new_source` 的题图或解析图。
- 已有正式卡本轮新提供、且尚未存在于仓库的附件。
- 唯一副本仍在 `/var`、`/private/tmp`、`/tmp`、剪贴板缓存或其他仓库外位置的来源。

来源固化输入使用 `math-fast-intake-source-stage-v1`：

```json
{
  "schema_version": "math-fast-intake-source-stage-v1",
  "study_date": "2026-07-18",
  "source_locator": "ID:112834 / 用户提供题目截图与标准解析截图 / 2026-07-18",
  "artifacts": [
    {"role": "question", "path": "/absolute/temporary/question.png"},
    {"role": "solution", "path": "/absolute/temporary/solution.png"},
    {"role": "solution_text", "path": "/absolute/temporary/solution.txt"}
  ]
}
```

角色只允许 `question`、`solution`、`solution_text`、`user_work`、`reference`。`solution_text` 只允许 UTF-8 `.txt` 或 `.md` 文件。writer 对其统一换行为 LF、去除首尾空白并固定一个结尾换行，再计算 SHA-256。其他 artifact 继续校验普通文件、扩展名与文件头、非空、单文件与总大小、重复文件和符号链接。随后按 `study_date + source_locator` 生成稳定 bundle ID，把文件原子复制到 `快速入库来源/YYYY-MM-DD/BUNDLE_ID/`，最后写 `manifest.json`。清单记录每个子文件的仓库相对路径、角色、MIME、大小和 SHA-256。

每个 fresh Capture 的 bundle 必须至少包含一张扩展名为 PNG、JPEG 或 WebP 的真实 `question` 图片，并且恰好包含一个 path-backed `solution_text` artifact。临时 source-stage 输入可使用绝对路径；持久 manifest、Capture 和 solution 文本不得保存本机绝对路径。

相同身份与相同内容返回 `noop`；相同身份出现不同文件失败关闭，不覆盖旧来源。清单及子文件都是不可变事实。固化成功但 capture 失败时允许保留来源包供同一 capture 重试，不得因此声称 capture 已成功。

同一来源包创建后不允许增量追加。若之后又出现新的解析图或作答图，必须使用新的稳定 supplemental locator 单独固化并记录为 source-backed 表示事件，夜间再依据真实身份将多个事件合并到同一正式卡。

## capture 输入

学习中先用 `apply_patch` 在 `/private/tmp` 新建一个紧凑 JSON，再运行 `record --payload-file PATH --consume-payload-file`。不要使用 PTY、管道、`printf`、heredoc 或内联 shell JSON。成功或 noop 后 writer 删除临时 payload；验证失败时保留同一文件供一次确定性重试。输入必须包含以下顶层字段：

```json
{
  "schema_version": "math-fast-intake-capture-v2",
  "attempt_id": "warmup:WQ-example:QI-example",
  "study_date": "2026-07-18",
  "target": {
    "kind": "formal_card",
    "formal_id": "GS-629",
    "source_locator": null,
    "source_hash_before": null
  },
  "score_event_id": "SCORE-000000000000000000000000",
  "requested_action": "record_recurrence",
  "thread_ref": "codex-task-or-thread-reference",
  "source_bundle": {
    "manifest_path": "数学一回滚复习系统/快速入库来源/2026-07-18/BUNDLE_ID/manifest.json",
    "manifest_hash": "<64 lowercase hex>"
  },
  "episode_evidence": {
    "solution_text": "经题源或当前讲解核验的解析文本",
    "user_answer_text": "用户本轮原始推导或作答",
    "teaching_turns": [
      {"speaker": "user", "kind": "reasoning", "text": "用户原始推理", "origin": "user_observed"},
      {"speaker": "assistant", "kind": "hint", "text": "本轮相关提示", "origin": "assistant_explained"}
    ]
  },
  "evidence": {
    "result": "wrong",
    "user_facts": [
      {
        "text": "用户本轮仍未识别第二条直线是两平面交线。",
        "origin": "user_observed"
      }
    ],
    "independent_correct_steps": [],
    "first_break": {
      "kind": "method_trigger",
      "text": "没有先把联立的两个平面方程识别为交线。",
      "origin": "user_confirmed"
    },
    "later_breaks": [],
    "hints_needed": [
      {
        "text": "提示后才调取两个法向量叉乘。",
        "origin": "user_observed"
      }
    ],
    "self_corrections": [],
    "mastery_score": 2,
    "mastery_source": "warmup_score",
    "score_basis": {
      "text": "方法入口未独立触发，提示后完成。",
      "origin": "source_verified"
    },
    "unresolved": []
  }
}
```

带 `score_event_id` 时，脚本从 `复习记录.jsonl` 认证：

- `attempt_id` 与学习日期。
- 实际交付卡，而不是 anchor 卡。
- 队列 ID 与队列项 ID。
- 分数、匹配模式、anchor ID 和作答前正式卡哈希。

capture 只引用这些小字段，不复制评分事件中的完整证据快照。

普通正式卡可以不带评分事件；脚本会验证正式 ID 唯一并记录当前文件哈希。所有 fresh Capture 的 `source_bundle` 必须只含 `manifest_path` 与 `manifest_hash`，writer 会立即重验清单及全部子文件。

fresh Capture 只接受 `math-fast-intake-capture-v2`；历史 `math-fast-intake-capture-v1` 事件只保留 replay 兼容，不迁移、不修改，也不能再次通过 `record` 写入。每个 fresh Capture 的非空 `episode_evidence.solution_text` 必须与唯一 `solution_text` artifact 的规范文本完全一致；缺失、空白、重复、哈希漂移或文本不一致时，writer 在追加 ledger 前失败闭合。

尚未分配正式 ID 的真实新题使用 `target.kind: new_source`，必须提供稳定 `source_locator`、已固化 `source_bundle`、非空解析文本和非空用户作答文本，并把 `target.source_hash_before` 绑定为同一个 manifest hash；夜间再查重和分配 ID。缺少任一项时 writer 拒绝 capture，因此不会调用 Luna。已有历史 `needs_user` capture 只能在 freeze 前通过 source-verified amendment 补入真实 manifest hash 与 `source_bundle`，不能把临时路径或未确认来源直接映射到任意旧卡。

Capture 必须保持 release-neutral，不得写入 release、activation、Dispatcher authority、MCP authority、消费状态或 handoff 状态，也不得把这些字段嵌入任意嵌套对象。

## 证据来源

每一条证据都必须带来源：

- `user_observed`：用户真实作答或表达中直接观察到。
- `user_confirmed`：用户明确确认。
- `source_verified`：由队列、评分、题面或正式文件直接验证。
- `assistant_inferred`：模型根据现有证据推断，不能冒充用户事实。
- `assistant_explained`：本轮由助手提供的讲解或提示。
- `unresolved`：暂时无法确认。

`user_facts` 至少有一条，且至少一条必须是 `user_observed` 或 `user_confirmed`。这项门禁防止只有模型推断、没有真实学习证据的事件进入夜间正式流程。

第一个断点与后续断点分开保存。断点类型只允许：知识、概念、条件、方法触发、方法、计算、表达、身份或未知。用户某一步正确，不等于整个方法已经掌握；`independent_correct_steps` 只记录已确认正确的局部步骤。

## 幂等与失败恢复

capture 的稳定身份由 `attempt_id + target identity` 生成：

- 同一身份、同一规范证据再次提交返回 `noop`，不追加第二条。
- 同一身份出现不同证据时失败关闭，不覆盖旧事实。
- 评分成功但 capture 失败时，保留评分事件 ID、attempt ID 和同一证据包，重跑一次即可。
- 一次重试仍失败时不得声称入库成功；应返回紧凑恢复胶囊，等待后续恢复。

写入在独占锁内完成，追加后立即重新读取、验证内容哈希并回放账本。坏 JSON、坏哈希、未知事件引用或重复关闭都失败关闭。

`quick_intake.py verify --date YYYY-MM-DD` 同时验证该日账本回放、每个有效来源清单及其全部子文件，并返回 `source_bundle_count` 与 `source_artifact_count`。它不是实时学习路径的额外必做调用，但可用于批次验收和故障排查。

若当天稍后发现 capture 证据需要修正，可以在 freeze 之前使用 `amend` 追加完整修订事件。原 capture 仍保留；已经关闭或仍属于 active freeze 的 capture 不允许直接修订。历史 `new_source` 可在 freeze 前通过 amendment 同时补入 `target_patch.source_hash_before` 与 `target_patch.source_bundle`；二者必须绑定同一份真实清单。尚未冻结的正式卡 capture 也可用 source-verified target patch 绑定当前唯一正式卡哈希，用于同卡前序收口改写后的版本 rebase；该 rebase 必须使用 `reason.origin: source_verified`，且规范化后的 `evidence` 必须与当前有效证据完全相同，只允许改变来源版本绑定。原始 capture 版本仍保留。freeze 会固定完整 amendment ID 顺序、有效证据哈希、有效目标哈希和有效来源包哈希。

`pending` 会返回顶层 `active_freezes` 和每个 pending capture 的 `active_freeze_ids`；跨任务恢复必须复用原 freeze，工具拒绝把同一 capture 放进第二个 active freeze。若核心事实在正式卡和回滚状态尚未变化时被推翻，用户明确确认后可运行 `abort-freeze --freeze-id ID --reason TEXT`。它追加 `freeze_abort`，不关闭 capture；之后才允许 amend 和新 freeze。若目标正式卡或回滚单元已经变化，自动 abort 失败关闭，必须先人工核对已写状态。

freeze 后若只是表示层补充，先用原 freeze 完成当前收口，再另记 `update_representation` capture；不得把同一次作答伪装成新的错误或复发。若新信息推翻题源身份、核心用户事实或本次是否做错，不能强行关闭互相矛盾的证据。

## pending 目标集

夜间开始时执行：

```text
python3 数学一回滚复习系统/scripts/quick_intake.py pending --date YYYY-MM-DD
```

输出是跨任务的权威目标集，不依赖当前聊天是否仍保留完整历史。输出按正式 ID 或新来源定位分组。同一卡同日多条 capture 在正式层只改写一次，但必须按时间保留每次事件的事实和断点。

## 夜间处理顺序

1. 用 `pending` 读取当天目标并完成只读查重、正式 ID 映射与来源核验；新附件直接使用 capture 中已经固化的清单，禁止回头依赖临时原图。
2. 在任何正式写入前，用 `math-fast-intake-freeze-v1` 调用 `quick_intake.py freeze`。freeze 记录 ledger 前缀、每个 capture 的原始哈希、amendment ID、有效证据与目标哈希、正式卡路径和修改前哈希；新来源还记录真实 artifact 哈希和 `created` / `merged` 身份裁决。
3. 用户应在启动夜间任务时选择所需的高能力模型；skill 本身不能切换模型。当前没有不可伪造的模型注入通道，持久回执固定写 `model: unknown`，不得读取或设置普通环境变量伪造模型来源。
4. 用该模型逐卡审查；只根据 freeze、事件和真实来源改写正式表示。同一正式目标只写一次。
5. 所有正式卡完成后只执行一次成功的 wrongnet rebuild。
6. 对做错或复发目标完成定向回滚；掌握候选、纯表示更新和否决裁决不得伪造 wrong event。
7. 关系层只保存 SHADOW 提案，不直接写正式关系。
8. Wiki 是正式 closeout 的必需层；Tutor 同步可选，不属于 closeout 门禁。对冻结目标执行 Wiki ingest，再做内置目标级 parity 与 lint。visual 只在正式卡确有视觉引用时为必需，否则明确 `not_applicable`。
9. 所有结构化产物通过后，用 `math-fast-intake-closeout-v2` 追加一次 closeout。

学习事实日期使用 freeze 的 `study_date`；wrongnet 实际构建日来自当前 `wrong_questions.json.updated_at`，经验证后记为 `artifact_date`。`artifact_date` 不得早于 `study_date` 或晚于当前日期，Wiki `last_updated` 必须与它一致。隔天补做昨天的夜间收口时，回滚复发日期仍是昨天的 `study_date`，派生产物日期则是今天的 `artifact_date`。

任一必需层失败时不追加 closeout，capture 保持 pending，可在下次重跑。夜间不得为了闭环而捏造没有证据支持的错因、掌握状态或关系。

## freeze 与 closeout 门禁

freeze 输入必须声明 `all_pending_for_date` 或 `explicit_subset`，并把每个 capture 恰好分配到一个正式目标。`existing_formal` 必须与 capture 的正式 ID 和当前来源哈希一致；`new_source_created` 要求冻结时该正式 ID 尚不存在；`new_source_merged` 要求冻结时目标卡唯一存在。新来源两种模式的 `source_binding.artifact_path` 必须指向 capture 的仓库内 manifest，`artifact_hash` 与 `resolved_source_hash` 都必须等于 manifest hash；来源 locator 必须一致。临时目录、仓库外文件、单独抽出的某张子图都不能替代清单。已有正式卡的新附件作为 `supplemental_source_bundles` 随 freeze 固定，不改变正式卡身份。

freeze 与 close 都会重验 manifest 结构、目录身份、每个子文件的路径、大小、文件头和 SHA-256。close 把 manifest 与每个子文件分别加入 `verified_artifacts`，在 `closeout_prepare` 后再次逐文件哈希；任一子文件被删除、替换或并发改动都会使 prepare 失效，capture 保持 pending。

freeze 同时保存目标卡已有的 `fast_intake_refs`、`fast_intake_source_refs`，以及目标回滚单元的修改前哈希、processed event 集合和调度历史 event 集合。返回值直接给出本批需要写入正式卡的规范 token：

- `fast_intake_refs`：`capture-v1:{capture_event_id}:{effective_evidence_hash}:{requested_action}`。
- `fast_intake_source_refs`：`source-v1:{来源 locator 与来源哈希的 canonical JSON SHA-256}`。

这些 token 必须作为正式卡 frontmatter 的扁平列表新增，不能只把同样字符串放在正文、注释或回执中。主来源与 existing-formal supplemental 来源都生成 `fast_intake_source_refs`。正式卡未变化时不得伪造新增 token；存在新的 source ref 时正式结果不得声明 `unchanged`。正式卡发生创建或更新时，freeze 前后新增 token 集合必须与该目标的冻结 capture 精确一致。`new_source_merged` 必须真实更新正式卡，来源清单及全部子文件在 close 时会再次哈希。

`math-fast-intake-closeout-v2` 不再接受自由文本 `passed` 或任意字符串回执，必须引用唯一 `freeze_id` 并包含：

- 每个正式目标唯一一条 `created`、`updated` 或 `unchanged` 结果，附当前路径、哈希和实际 changed fields。
- 每个 capture 唯一一条与 `requested_action` 匹配的 outcome。做错与复发必须更新或创建正式卡，并绑定 `复习单元.json` 中 freeze 后新增的 processed event 和 unit canonical hash；复发还必须绑定唯一的本日 `正式错题复发` 历史条目。表示更新、掌握确认、掌握否决按正式卡或 closeout 绑定，不得制造错题事件；掌握确认还必须与现有回滚单元状态一致。已提交且完整性回放通过的 `mastery_confirmed`，仅在 freeze 绑定的 effective evidence 同时满足 `result=correct`、`mastery_score>=4`、至少一条 `independent_correct_steps` 来自用户观察或用户确认且 `hints_needed` 为空时，才可被调度器解释为永久交付排除证据；存在 amendment 时不得退回读取原 capture 的旧证据。
- wrongnet 九个生成物的当前 SHA-256、目标 projection hash 和一次 rebuild 工作流声明。close 会用 canonical wrongnet parser 从最终正式卡重算目标 projection，再与主快照比较；该声明不是命令执行次数的密码学审计。
- 每张目标卡的 Wiki source summary 当前哈希及 `formal_projection_sha256`。close 会重算目标级 subject、knowledge、methods、error causes、source/index/coverage/matrix 唯一性、稳定簇引用、成员关系、控制字符和必要字段。
- 关系层的逐目标 SHADOW 裁决，以及 visual 的逐目标 `passed` 或 `not_applicable` 回执。

close 从规范化到提交全程同时持有正式收口锁和账本锁。它先追加 `closeout_prepare`；prepare 不关闭 capture。随后再次哈希所有已验证产物，只有最终复核通过才追加 closeout commit，回放时也只有 commit 会写入 `closed_by`。复核失败会失效 prepare，capture 保持 pending，同一原 freeze 可按新 generation 重试且事件 ID 不冲突。该互斥保证只覆盖同样遵守正式收口锁的 writer；绕过锁的外部进程不在可证明边界内。

`all_pending_for_date` 只要求 freeze 创建时覆盖当日全部 pending。freeze 后新增的无关 capture 留给后续批次，不使已经开始正式工作的原 freeze 失效；同一正式目标的后来 capture 若仍绑定改写前版本，close 会失败并要求对未冻结 capture 做 source-verified 版本 rebase，避免它永久 pending。原冻结 capture 自身、amendment、证据或目标发生变化仍会失败关闭。完全相同且仍有效的成功回执以 `request_hash` 返回 `noop`，即使正式卡后来发生了其他合法变化，也不会重复关闭。当前 closeout 没有受信模型注入通道，因此 `model` 固定写 `unknown`；模型选择由用户在任务外完成，不能由收口 Agent 自报高能力模型名称。

只有这些门禁全部满足，`close` 才追加 `nightly_closed` 回执。原 capture、amendment 与 freeze 永不改写；任何失败项继续 pending。

## 完成措辞

白天成功后只能说：

```text
快速记录成功，已进入今晚的正式优化队列。
```

不得在白天声称“正式卡、回滚、网络和 Wiki 已全部更新”。夜间全部门禁通过后，才可以说“夜间正式收口完成”。
