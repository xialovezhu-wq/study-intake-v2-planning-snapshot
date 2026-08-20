# Vocab Export Response Contract

Use plain-text labels in chat. Omit empty global sections, but keep every required field for each included card.

```text
本次结果

文章来源：
source_id：
complete_receipt_id：
capture_status：created / idempotent_noop
effective_capture_count：
候选总数：
A 类：
B 类：
C 类：
已掌握排除：
长期库已有：
待补充来源：

A 类

按 bbdc-export-format.md 输出每个完整卡片。

B 类

按 bbdc-export-format.md 输出每个完整卡片。

C 类

按 bbdc-export-format.md 输出每个完整卡片。

不导入但值得保留

item：原因；来源位置

已掌握或重复项

item：状态；依据

长期库匹配

新候选：
已在长期库：
疑似重复：
需要正式入库复核：

生成例句门禁

四层地基通过：
needs_context：
needs_user_evidence：
needs_reference_graph：
待审核参考缺口：
未生成例句的 item：

正式数据保护

master_bank 未修改。
mastered_items 未修改。
sentence_patterns 未修改。
review 未修改。
真实 article 未修改。
formal_write_count：0

后台状态

article_completed 已提交；Luna 将由后台轮询处理，本次 A/B/C 输出未等待 Luna。

下一步

如需处理 YYYY-MM-DD 的正式入库，单独发送：开始 YYYY-MM-DD 英语正式入库
```

Rules:

- Lead with the source and classification result.
- Do not list every file read or every tool call.
- Do not use Markdown emphasis or hash headings in chat.
- Never invent a source sentence, user-unknown status or reference ID.
- Missing source sentence produces needs_context and excludes the item from A/B generated-example output.
- Missing current user wording and explicit unknown, mistranslated or missed evidence produces needs_user_evidence and omits the generated example.
- Missing SP fit uses 可选句式卡：无; it does not waive作文句型、作文词组 or verified syllabus evidence.
- All old words mastered produces 相关旧词已掌握，不再调用.
- If no formal file changed, one compact protection block is sufficient.
- Never claim article completion without a real complete-article receipt. Do not start, poll or wait for Luna before returning the A/B/C output.
