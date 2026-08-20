# CSV 入库模板

本文件只保留 13 列正式库形状和语义检查规则，供日终 typed-action 编纂参考。kaoyan-english-intensive-reading 不执行这里的写入步骤。正式变更只由精确触发 `开始 YYYY-MM-DD 英语正式入库` 后的 kaoyan-english-daily-intake-curation 生成 typed actions，再交给 deterministic writer。

明确不会、误译、长期价值、普通“正式入库”或白天对话都只建立 quick-capture 候选，不构成正式写入授权。

## Typed action 前检查

1. 读取 `prompts/codex_prompt.md`、`schema/schema.md`、目标文章页。
2. 读取 `bank/mastered_items.csv`；已掌握 item 不重新写入。
3. 读取 `bank/master_bank.csv`；按 `item.strip().lower()` 查重。
4. 确认每个候选都有 `source_article` 和 `source_sentence`。
5. 普通生词、专名、低价值背景词先保留为候选，不写 CSV。

## 字段

```csv
id,date,type,item,source_article,source_sentence,meaning,usage,writing_value,tags,review_note,appear_count,last_seen
```

## 允许值

`type`：

- 单词
- 词组
- 熟词僻义
- 句型
- 长难句
- 写作表达

`writing_value`：

- 适合
- 一般
- 不建议

`tags`：多个标签用 `|`，优先使用项目已有标签，例如 `阅读高频`、`熟词僻义`、`写作友好`、`仅阅读识别`、`长难句结构`、`固定搭配`、`易混表达`、`题目定位`、`老师强调`、`回滚复习`。

## 新增行字段草稿

```csv
YYYYMMDD-001,YYYY-MM-DD,type,item,source_article,source_sentence,meaning,usage,writing_value,tags,review_note,1,YYYY-MM-DD
```

## 已有 item 更新判断

如果 `item` 已存在：

- 不新增整行。
- 用户只授权“入库”而没有明确授权更新既有项时，只报告 `existing_bank`，不改任何字段。
- 只在用户明确要求更新该条目时，才修改被授权的 `appear_count`、`last_seen`、`tags`、`usage` 或 `review_note`。
- 更新时不覆盖旧语境证据，写后报告实际变更字段。

## Deterministic writer 安全

- Sol 只产 `master_bank_insert` 或 `master_bank_update` typed action，不直接调用 CSV 库或编辑文件。
- deterministic writer 负责字段转义、原子写入和写后 13 列检查。
- Lint 发现重复或规则外 type 时只报告；合并和改字段需要用户确认。
