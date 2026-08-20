# Sentence Response Contract

Use the smallest shape that satisfies the user's request.

## Learning mode

Do not emit the full explanation template before the user answers. Use:

```text
你的第一遍翻译

原样引用用户提供的第一遍翻译，不润色、不改写。用户未提供时省略本区，不补造。

当前判断

用陈述句说明用户已经做对的部分，以及目前证据能定位到的第一个不确定点。本区不使用“是否、能否、会不会、哪一个、什么、为什么、怎么”等疑问构式。改写为“当前证据不足以确认……”。

本轮问题

只问一个能够区分词义、结构、逻辑或方法断点的问题。整条回复中只有这里出现诊断性问句。
```

Do not include the final translation, answer, A/B/C list or multiple questions when the user requested guided thinking or answer protection.

## Direct explanation mode

```text
结论

说明当前翻译或理解是否正确。

推荐翻译

给出自然中文翻译。

结构

主干：
修饰或从句：
逻辑关系：

关键搭配

item：本句意思和用法。

第一个断点

哪里正确：
从哪里偏离：
偏离原因：
下一动作：

候选判断

item：长期库候选 / 不背单词候选 / 不建议入库
依据：
后续位置：

快速入库回执

receipt_id：
capture_id：
status：created / idempotent_noop / capture_saved_but_projection_failed / failed
event_sha256：
projection_status：rendered / failed
formal_write_count：0
formal_writeback：none
```

Rules:

- In the first learning turn for a sentence, quote the user's first translation exactly when present. Do not silently replace it with a polished version or invent one when absent.
- Keep observations declarative. Do not use whether/can/which/what/why/how constructions or restate the diagnostic as a second indirect or rhetorical question before the one question under 本轮问题.
- Do not use Markdown emphasis or hash headings in chat.
- Omit sections outside the user's requested scope.
- Do not output a full BBDC list or CSV rows in per-sentence mode.
- Daytime sentence work uses 长期库候选 and the quick-capture receipt; it never labels a proposal as a completed formal entry.
- Omit the receipt block until the sentence is resolved. Never synthesize a receipt when the CLI did not return one.
- For a question-location sentence, explain the local trap without revealing the correct option unless explicitly asked.
- When the user's translation is already correct, do not fabricate an error to fill the breakpoint fields.
