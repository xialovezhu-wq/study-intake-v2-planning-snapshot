# Candidate Tracking

Use this when deciding where current-sentence words, phrases, structures, and errors should go.

## Check Order

1. Bind the observation to the active `source_id`, `sentence_id`, exact source sentence and its `sentence_hash`. Keep the article-level `source_hash` separate.
2. Once the sentence is resolved, persist the observation through the quick-capture CLI. Do not delay foreground capture for Luna or nightly formal matching.
3. Treat the article candidate area as a human-readable projection, not the canonical capture receipt.
4. Let the nightly curation path check `mastered_items.csv`, `master_bank.csv` and `sentence_patterns.md`; the daytime skill records proposals with `formal_write_count=0`.

## Article Candidate Tables

```md
### 本句抠词 / 词组候选

| sid | item | 类型 | 本句意思 | 为什么抠出 | 后续处理 |
|---|---|---|---|---|---|
| Sxx | 待补充 | 单词/词组/熟词僻义/写作表达 | 待补充 | 用户明确不会/误译/影响理解/固定搭配/写作价值 | 长期库候选/不背单词候选/不建议 |

### 本句句型 / 结构候选

| sid | 结构 | 原句对应 | 用户问题 | 复习提醒 | 后续处理 |
|---|---|---|---|---|---|
| Sxx | 待补充 | 待补充 | 待补充 | 待补充 | SP候选/CSV句型候选/仅文章候选 |
```

## Decision Labels

- `长期库候选`: high value and source-backed, but nightly formal curation has not completed.
- `不背单词候选`: useful for recognition/export but not worth long-term CSV.
- `不建议入库`: proper name, simple background word, low transfer value, already mastered, or not exam-useful.

## High-Value Signals

- User explicitly missed, mistranslated, or could not translate the item.
- It affects main idea, question location, option judgment, contrast, cause, condition, concession, or attitude.
- It is a familiar word with a new meaning.
- It is a fixed phrase or reusable collocation.
- It carries writing value.
- It exposes a long-sentence structure trap.

## Sentence Pattern Split

Ask: after removing the concrete verb, is the structure still teachable?

- Yes: sentence-pattern card candidate in `bank/sentence_patterns.md`.
- No: CSV `type=句型` candidate, usually a verb-dependent template such as `force sb to do`.

Do not create more than the project allows. Capture uncertainty explicitly; do not turn a daytime proposal into a formal write.
