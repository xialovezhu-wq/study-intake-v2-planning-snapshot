# A/B/C 导出分层契约

## Goal

把文章候选按当前 BBDC 导出价值分为 A、B、C，同时保持来源、掌握状态、正式库状态和写入授权彼此独立。

A/B/C 表示导出优先级，不等于写入 master_bank。

## Success criteria

- 每个 item 有真实 source_sentence；缺失时明确 needs_context。只在生成新例句时，缺用户措辞和明确错词证据另标 needs_user_evidence。
- mastered items 不进入任何导出层。
- 当前候选去重。
- 已在 master_bank 的 item 标为 existing_bank，不伪装成新条目。
- A/B/C 理由来自文章作用、用户证据和迁移价值。
- 旧词联动强度只在语义自然且符合记忆曲线时计入。
- 普通低价值词不会因“出现过”自动升为 A。
- 不修改正式数据。

## Pre-filter

1. 排除 bank/mastered_items.csv 命中项。
2. 对比 bank/master_bank.csv，标记 existing_bank。
3. 忽略大小写和首尾空格合并当前重复候选。
4. 去掉专名、来源元数据和无真实语境的普通词。
5. 缺 source_sentence 时标记 needs_context；不进入 A/B 例句生成。
6. 新例句缺用户当前措辞，且没有 unknown、mistranslated 或 missed 类明确错词证据时，标记 needs_user_evidence 并留空例句。
7. 把 ordinary、低迁移价值且不影响理解的词留在 C 或 article_only。

## A

A 表示本次导出优先级最高。满足一个强信号即可：

- 影响主旨、题目判断或关键逻辑。
- 位于定位、选项对应、转折、因果、条件、让步或观点句。
- 用户明确不会、误译或无法翻译。
- 熟词僻义。
- 高频抽象名词、常见考研阅读动词或关键固定搭配。
- 在用户错误、解析或文章逻辑中反复出现。
- 与 active 旧词存在自然且到期的强联动。

强信号仍需真实来源。A 不自动写 formal CSV。

## B

B 表示值得导出但优先级低于 A：

- 有稳定阅读价值但不决定题目。
- 常见搭配、主题词或可迁移词组。
- 适合未来识别，写作价值一般。
- 有自然但较弱的旧词联动。
- 句子级候选有价值，但证据不足以进入 A。

## C

C 表示可选、暂缓或只阅读识别：

- 极简单普通词。
- 只是背景词，不影响理解和做题。
- 过于口语、低频、专名或不适合迁移。
- 用户短暂卡住但现已能稳定识别。
- 值得留在文章语境，但当前不值得 BBDC 优先导出。

C 默认不生成旧词联动例句。

## Status labels

- new_candidate：未掌握、不在 master_bank、有来源句。
- existing_bank：已在 master_bank，且未被 mastered 排除。
- mastered_excluded：已掌握，不导出。
- duplicate_candidate：当前文章或会话重复。
- article_only：只保留在文章。
- needs_context：缺真实来源句，暂不进入 A/B 例句生成。
- needs_user_evidence：有来源句，但新例句缺用户当前措辞或明确错词证据。

## Stop rules

- 全部候选都被 mastered 排除：报告排除并停止。
- active article 或来源句缺失：只询问最小上下文。
- 候选只能靠生成改写或 NotebookLM 总结支持：降为 needs_context。
- 分层完成后停止，不自动进入正式写入。
