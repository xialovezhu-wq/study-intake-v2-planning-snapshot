# 不背单词导出格式

Use this format for article-end BBDC or 不背单词 A/B/C output. The original source card and any system-generated old-word example are separate evidence objects.

## A and B card

```text
item：本文含义

原句：
原句中文：
用法：
一句话笔记：

旧词联动状态：通过 / needs_context / needs_user_evidence / needs_reference_graph / 待审核参考缺口 / 本项无需生成
用户措辞或错词依据：
作文句型依据：来源 ID｜结构名｜approved/corrected
作文词组依据：来源 ID｜item｜approved/corrected
大纲词汇依据：occurrence ID｜item｜verified 状态
可选句式卡：SP-NNN｜骨架 / 无
旧词联动例句：
例句中文：
结构拆解：
复习旧词：
约束与自然度检查：
```

Rules:

- 原句必须来自 article、option、explanation、video timestamp 或用户提供的真实来源，不能生成。
- 原句中文自然、准确，并保持文章语境。
- 用法说明本语境词义、搭配或熟词僻义。
- 一句话笔记短而可复习。
- 只有生成新例句时才填写四层地基字段。
- 新例句必须遵守 schema/reference_grounded_examples.md。
- 缺 source_sentence 时写 needs_context，不输出最终新例句。
- 缺少用户当前措辞且没有 unknown、mistranslated 或 missed 类明确错词证据时写 needs_user_evidence，不输出最终新例句。
- 图谱过期且重建验证失败时写 needs_reference_graph。
- 两个合规地基包仍不自然时写待审核参考缺口。
- 不从模型记忆补造作文、大纲或 SP ID。
- SP 仅在结构逐节点匹配时使用，否则写无。
- 复习旧词只使用排除 mastered 后的 active 正式条目，并注明 D1、D3、D7、D15、D30、D60、D90+ 或 oldest-fallback。
- 系统生成例句不更新 mastery、appear_count 或 last_seen。

## C card

```text
item：本文含义

原句：
原句中文：
用法：
一句话笔记：
不优先入库理由：
```

C 级默认不生成旧词联动例句；如果用户单独要求，仍需通过四层地基。

## Required summary

```text
不导入但值得保留

item：原因；来源位置

已掌握或重复项

item：该词已证明掌握 / 已在 master_bank / 当前候选重复；依据

长期库匹配

新候选：
已在长期库：
疑似重复：
需要正式入库复核：

掌握排除

已排除：
说明：

生成例句门禁

通过：
needs_context：
needs_user_evidence：
needs_reference_graph：
待审核参考缺口：

正式 CSV 下一步

本清单不自动写 master_bank。如需处理指定日期，另行发送：开始 YYYY-MM-DD 英语正式入库。
```

## Output priority

Preserve source evidence, classification, mastered/duplicate status and grounding failures first. If the list is long, output in batches. Do not collapse per-item cards into a table when that would remove required evidence fields.

In chat, use plain-text labels with no Markdown emphasis or hash headings.
