# Visible intake output

Follow workspace chat formatting. Use the template matching the actual terminal
state; never merge fast capture and formal completion wording.

## Fast fact capture

```text
快速捕获结果

Capture ID：{CAP-ID}
学习日期：{YYYY-MM-DD}
稳定证据：{saved locator count and hash status}
身份提示：{existing formal ID | new candidate | unknown}
事实状态：已保存
质量状态：{待日终编纂 | 已捕获未授权}

待补充

{none or facts left for daily curation}
```

Do not say “正式入库完成”, “高质量入库完成”, or “全部审计通过”. Do not include
related old questions, relations, topic chains, or a global audit inventory. If the
stable evidence write failed, report the persistence blocker and no capture ID.

## Full formal curation

```text
正式编纂结果

正式节点 ID：{ID or no-write}
来源 Capture ID：{CAP-ID | full-now}
路径：{A-G and outcome}
主知识点：{code and name, when applicable}
用户事实：{preserved summary and provenance}
详情入口：{durable entry or pending}

写入与关系

变更表面：{formal and derived surfaces}
关系状态：{connected | reviewed_no_reliable_edge | needs_user}
复盘输出：未读取、未生成、未修改

校验

preflight：{PASS | not run | FAIL}
apply：{COMMITTED | ALREADY_COMMITTED | zero calls}
closeout：{COMPLETE | failed branches}
receipt：{path or none}

7 天外强关联旧题回顾

{0-5 items, or the required no-eligible-candidate statement}

待补充

{none or the smallest blocking fact}
```

Do not mark a daily capture `curated` until the receipt exists and required formal
closeout passes. An `already_current` result binds a formal ID and verification hash,
not a fabricated receipt. For receipt-bound failure, identify the receipt and resume
action without suggesting another apply. `full_now` never records a capture result.
