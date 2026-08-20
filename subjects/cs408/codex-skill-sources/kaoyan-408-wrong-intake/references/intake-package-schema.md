# 408 结构化入库包（intake_package.json）字段契约

本文件只用于 `full_now` 或日终批次中的一个 `curate_one` 正式节点，不用于学习中
事实捕获。快速捕获使用 `fact-capture-schema.md` 和
`scripts/intake_fact_capture_408.py`，不会生成半成品正式 package。

模型只负责高质量判断并产出这个包；落表、索引同步、复习单元、安全卡由
`scripts/intake_apply_408.py` 一次性完成。该公开入口强制恰好一个 package，内部复用
`intake_batch_apply_408.py` 的仓库锁、WAL 与幂等实现，不能绕过事务边界。
包写到临时目录（如
`/tmp` 或会话 scratchpad），不要存进 408 仓库。

## 流水线

```bash
# 0. 生成锁外 provisional ID 建议（可选；不是最终分配）
python3 scripts/intake_preflight_408.py --suggest-id CO --year 2014 --repo .

# 1. 同题只读 fan-in 生成唯一包 → 2. 编排器完成 preflight、一次 apply 与并行 closeout
python3 scripts/intake_parallel_orchestrator_408.py /tmp/pkg.json --repo . \
  --lock-timeout 30 --idempotency-key sha256:...

# normal receipt 已存在但 closeout 失败时，只重跑收尾
python3 scripts/intake_parallel_orchestrator_408.py \
  --resume-receipt /absolute/path/to/receipt.json --repo .
```

preflight FAIL 时 apply 调用次数为 0；先修包，再重跑。closeout FAIL 且已有 receipt 时禁止重跑 apply。

日终从 capture 生成 package 时，必须先绑定 capture ID、事实快照哈希和稳定证据
哈希。模型可以改写下表中的派生语义字段，但不得静默改变用户事实、来源/details
身份、学习日期、new/redo 事实、复做次数或调度状态；冲突字段保持零写入并转
`needs_user`。正式 apply 成功后只把 receipt SHA-256 写回捕获账本，不把绝对 receipt
路径写入 tracked ledger。

## new 模式字段（新题正式入库，A/B 路径）

| 字段 | 类型 | 说明 |
|---|---|---|
| mode | str | `"new"` |
| formal_id | str | 同步单题包必须是 `DS/CO/OS/CN_年份_三位序号`；年份未知用 `UNK`。preflight 只给 provisional 建议，锁内会依据最新总表重新校验，只有事务成功后该 ID 才权威；公开同步入口不接受多题 coordinator manifest |
| source_id | str | `VISUAL_PENDING`、历史 ID（`HCO_0077`）或其他可回溯来源标识 |
| year | str | 与 formal_id 中年份一致；未知 `"UNK"` |
| subject | str | `DS`/`CO`/`OS`/`CN` |
| module | str | 主模块 `编码 名称`，须与主知识点同章，如 `CO03 存储器层次结构` |
| main_knowledge | str | **恰好一个**，`编码 名称`，必须在 `知识点标签表.md` |
| sub_knowledge | list[str] | 可空；每项都要在标签表 |
| hit_knowledge | list[str] | 命中知识点；缺主知识点时 apply 自动补入 |
| question_type | str | 如 `单项选择题` / `综合应用题` |
| core_point | str | 核心考点一句话，不带答案结论 |
| safe_summary | str | 安全题目摘要：题目骨架，不是完整题干 |
| key_parameters | str | 关键题设参数：量级/口径，不写具体到能反推答案的完整数据 |
| ask_type | str | 问法类型 |
| user_error_entry | str | 用户错误入口：`YYYY-MM-DD 入库：…` 开头 |
| redo_first_action | str | 复做第一动作 |
| fuzzy_concepts | str | 模糊概念，`A vs B；C vs D` 形态 |
| error_tags | list[str] | `E01-E10 名称`（对标 `错因标签表.md`）或恰为 `["暂无明确错因"]`；不得混用 |
| detail_entry | str | durable 详情入口；**禁止** `/var/folders/...` 等临时路径；详情卡存在时 apply 自动追加 `详情库：obsidian://…` 尾巴 |
| attachment_registry | object | 五个必备键：`题图`、`解析图或解析正文`、`作答痕迹`、`可视化详情入口`、`当前附件来源`；值用 已接收/未提供/待补充 语义 |
| first_done_date | str | `YYYY-MM-DD` 或 `未记录` |
| latest_review_date | str | `YYYY-MM-DD`（通常是今天） |
| latest_error_record | str | `YYYY-MM-DD 入库：…` 错误摘要，不含答案 |
| relation_candidates | list | 见下；可为空数组（不强行连边） |
| topic_chain_candidates | list[str] | 可作为独立维护提示；专题链不由单题 apply 自动写入 |
| review_unit | object | 可省略字段走默认；`内容名称`（默认核心考点）、`重要程度`（默认 4）、`难度等级`（默认 B中等）、`复习方式`（默认闭卷复述复做第一动作）、`备注` |
| date_source | str | 可选；默认 `用户 {latest_review_date} 入库确认` |
| redo_action | str | 可选；默认 `今日入库/复做` |

relation_candidates 每项：

```json
{"from": "CO_2014_002", "to": "CO_2015_005",
 "type": "R01 同一核心考点", "strength": "强",
 "reason": "两题都考…（写真实共享机制，≥10 字，不是空话）",
 "priority": "高"}
```

- `to` 必须已在节点总表；禁止 `H*` 历史 ID 端点、禁止自环。
- `type` 对标 `关系规则.md` R01-R09；R07/R08 默认弱、R06/R09 默认中，
  标"强"会触发 WARN，必须在 reason 给出证据。

## redo 模式字段（已有节点再次做错，C 路径）

```json
{"mode": "redo",
 "formal_id": "CO_2014_001",
 "latest_review_date": "2026-07-04",
 "latest_error_record": "2026-07-04 复做再次错误：…",
 "error_tags": ["E02 条件遗漏"]}
```

- `error_tags` 可选：只列**新增**错因，apply 合并进现有标签。
- redo 本体与关系边变更必须拆成两个独立正式维护动作。
- apply(redo) 同步 7 处：节点总表、科目节点库镜像、错题日期索引、
  错题复做记录（追加）、安全卡用户错误入口与错因、复习单元
  （错误次数 +1、回滚重置 D1）、命中索引主知识点小节复做日期。

## batch apply 的写入范围（new 模式，一次 9 处）

节点总表.md / 节点库/{科目}节点.md / 年份索引.md / 知识点命中索引.md
（主小节 + 每个命中小节，缺小节自动按编码序新建）/ 错题日期索引.md /
错题复做记录.md / 复习单元总表.md / 同名安全节点卡片 {ID}.md /
关系边表.md（`--no-edges` 跳过）。coverage、bridge、全局审计和旧题回顾在事务锁外由
单题并行编排器按依赖图各运行一次。

batch apply **永不**写：`复盘输出/`、`专题链/`、`概念词典.md`、`历史错题归档/`、
`wiki/`。专题链与概念词典按判断人工维护；历史映射走历史题路径。

事务性：仓库级单锁、WAL、before/after hash、receipt 与 idempotency key；任何一步
失败则整批不落盘或按 journal 恢复。batch 默认从不刷新 bridge，`--no-bridge` 仅为
兼容声明。`--dry-run` 先看 projected master，但不能替代锁内重新校验；SHADOW
dry-run 遇脏/未终态 WAL 必须失败，先显式 recover-only 并核验 receipt，不能暗中恢复。
