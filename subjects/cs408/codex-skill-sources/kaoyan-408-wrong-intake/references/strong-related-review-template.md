# Answer-safe related-question item

Use only after `related_candidates_408.py` has applied date filtering, source
lookup, and redaction.

```text
正式节点 ID：
Obsidian 打开链接：
来源 ID：
年份：
首次做题日期：
最近复做日期：
最近错误记录：
核心考点或旧题概况：
关系类型：
联系强度：
推荐原因：
历史归档 ID：
定位标记：
段落号：
完整来源位置：
复做保护：已隐藏答案、选项和既往作答信息
```

## Field rules

- Use a formal ID as the node identity. A historical ID is a source locator only.
- Historical sources require locator mark, paragraph, and full source position from
  `历史错题归档/定位索引.md`; use `定位标记：未提取` when absent.
- Non-historical sources use `历史归档 ID：无` and describe the available source.
- The old-question summary comes from the node's safe core point, knowledge point,
  and error record, never a copied stem or explanation.
- Preserve useful source structure but replace any answer-bearing fragment with
  `答案信息已隐藏（复做保护）`.
- Do not output candidates reviewed within 7 days unless the user explicitly names
  them or requests recent questions.

When no strong or medium candidate survives, output exactly the meaning:

```text
当前无符合 7 天外硬规则的强或中关联旧题，不用近 7 天题凑数。
```

Weak date-fallback candidates require an explicit user request and do not justify a
new relationship edge.
