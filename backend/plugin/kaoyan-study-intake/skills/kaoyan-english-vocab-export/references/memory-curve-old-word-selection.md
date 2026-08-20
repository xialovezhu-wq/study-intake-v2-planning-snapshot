# 记忆曲线旧词选择契约

## Goal

为新词选择今天最值得复现、且能自然进入同一句的 active 旧词。这个契约只负责旧词候选排序；最终新例句仍需通过 schema/reference_grounded_examples.md 的四层地基。

## Success criteria

- mastered items 已排除。
- 候选来自 master_bank，不由模型补造。
- 缓存经过当天日期和正式来源状态验证。
- 优先选择到期且自然的旧词。
- 无到期自然候选时才使用 oldest-fallback。
- 最近反复出现的词降权。
- 选择结果带 due 标签和 last_seen 证据。
- 生成例句不会自动更新任何正式数据。

## Candidate source

优先使用当天有效的：

- wiki/old_words/memory_curve_active_items.csv
- wiki/old_words/memory_curve_summary.md

有效条件：

- generated_date 为今天。
- 输入哈希或来源状态与当前 master_bank 和 mastered_items 一致。
- active_status=active。

缓存缺失或过期时，从 bank/master_bank.csv 和 bank/mastered_items.csv 实时重算；不要沿用旧结论。

可选重建命令：

    python3 scripts/build_old_word_memory_curve_index.py --today YYYY-MM-DD

重建派生索引不授权写正式数据。

## Selection

1. 排除 mastered_items 中的所有 item。
2. 从 master_bank 建立 active 候选池。
3. 使用 last_seen 计算 days_since_seen；缺失或异常时回退 date。
4. 只保留能与当前目标词自然共句的候选。
5. 按复习窗口标记：
   - D1：1 天
   - D3：2 至 4 天
   - D7：6 至 8 天
   - D15：13 至 17 天
   - D30：26 至 34 天
   - D60：52 至 68 天
   - D90+：80 天以上
6. 多个候选到期时，先按自然度，再按更久未出现排序。
7. 没有到期且自然的候选时，选择最久未出现但仍自然的 active item，并标记 oldest-fallback。
8. 今天或昨天出现的词降权，除非没有其他自然候选。
9. 每个新词使用 1 至 3 个旧词；一条自然句优先于堆叠。
10. 全部自然候选已掌握时，输出 相关旧词已掌握，不再调用。
11. 没有自然 active 候选时，输出 旧词库暂未发现强关联词。

## Output trace

每个被选旧词记录：

- item
- 中文义
- 依据：D1、D3、D7、D15、D30、D60、D90+ 或 oldest-fallback
- last_seen 或回退 date
- 来源 ID

## Stop and write boundary

- 候选池为空或没有自然联动时停止，不编造旧词。
- 排序完成后转四层地基契约生成例句。
- 旧词出现在系统生成例句中，不更新 appear_count、last_seen、mastered_items 或 review。
- 只有单独明确的正式维护任务才能写回。
