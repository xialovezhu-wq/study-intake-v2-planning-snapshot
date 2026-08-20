---
name: kaoyan-math-visual-review
description: "Create a 10–15 minute math daily-review narrative, structured Markdown, or local HTML artifact from verified study evidence. Use for 数学日终复盘, 生成可视化复盘, 10–15 分钟口播稿, or 把今天数学学习整理成 HTML/复盘稿. Ordinary review requests stay read-only; an explicit request for HTML, saving, or a finished artifact authorizes one safe review artifact, not edits to formal cards. Route single-question intake to kaoyan-math-wrong-intake and explicit unified redo selection to kaoyan-math-daily-filter. Chronicle may locate recent context but never serves as the sole evidence for IDs, wrong causes, time, mastery, or redo results."
---

# Kaoyan Math Visual Review

## Goal

把已验证的当天学习证据压缩成一条清晰复盘主线：今天解决了什么、错误模式是什么、第一动作缺在哪里、重点题如何说明问题、明天先做什么。

## Success Criteria

- 成品能在 10–15 分钟内讲完，重点围绕具体证据和下一步动作，不复制长题干或完整解析。
- 所有题号、错因、method_gap、掌握度、时长和复做结果都来自用户输入或稳定本地文件；缺失项明确标记。
- 单题正式写入和统一筛题分别交给对应技能；本技能只消费已经确认的卡片或 shortlist。
- 普通复盘不修改正式卡、`生成/`、回滚账本、wiki 或 Tutor。
- HTML 成品无外部依赖、可本地打开，并在交付前完成渲染检查。

## Authorization And Route

- “总结、复盘、口播稿”默认只在对话中输出。
- 明确要求“HTML、保存、生成成品”时，可在已存在的安全 review/wiki 目录创建一个成品；若没有已批准目录，先返回草稿并询问是否创建目录。
- 只有用户明确要求同步 Tutor 或刷新 StudyVault 时，才执行 `references/tutor-review-closure.md`。
- 混合请求中，当前题入库转 wrong-intake；显式统一筛题先转 daily-filter，再用其结果做复盘。

## Evidence Budget

先使用用户摘要、命名 formal IDs、现有 daily-filter 结果和相关 wiki/method_gap 页面。只有核心事实缺失时才扩大检索：

1. 用 Obsidian CLI 定位正式卡、wiki、backlinks 或安全保存位置；
2. 用户依赖“今天、刚才、当前屏幕”时，用 Chronicle 识别可能的主题、可见 ID 和应用；
3. 回到正式卡、wiki、视觉详情或用户文件验证最终事实。

独立读取可以并行；收齐后先形成一个规范化证据表，再写叙事。已有证据足以支持核心复盘时停止检索，不为增加例子或润色继续搜索。

普通单组复盘保持单 Agent。只有输入能清晰拆成多个互不重叠的证据组，或复杂 HTML 需要独立只读视觉 QA，且真实子代理能力可用时，才把证据整理或渲染审查作为只读子任务并行；主 Agent 负责最终叙事、文件写入和验证，不派多个 Agent 重复评价同一材料。

## Output Route

根据请求选择一种主成品：

- Structured Markdown：脚本、提纲或聊天复盘。
- Single-file HTML：默认视觉成品；纯 HTML/CSS/Vanilla JS。
- 分离 HTML/CSS/JS：仅用户明确要求。

具体结构见 `references/output-structure.md`，视觉合同见 `references/html-review-template.md`，保存与保护边界见 `references/safety-boundaries.md`。

程序化处理只用于去重、排序、统计和把大批结构化证据压缩成小表；叙事判断、缺失证据处理、数学表述和最终验证使用直接模型判断。

## Stop Rules

- 无法确认日期、题号、错因或学习时长时写“待确认”或“未记录”，不从屏幕或上下文猜测。
- 少于 5 道可靠重点题时按实际数量输出，不补虚构卡片。
- 安全保存位置不存在且用户未授权创建目录时，交付对话草稿并停止写盘。
- HTML 必须渲染检查布局、裁切、移动端和内容完整性；无法渲染时说明原因和次优检查。

## Completion

报告证据来源、缺失信息、输出格式、是否保存及路径、是否使用 Obsidian 或 Chronicle、是否显式同步 Tutor，以及正式卡、生成物和回滚账本未被修改的确认。

## Local read-only MCP route

Eligibility is true only for cold review evidence with stable formal IDs: formal-card summaries, the bounded activity window, direct relations, and review snapshots. When eligible, the only allowed MCP binding is the math-only server `kaoyan_math_read`, tool `math_read_bundle`, exposed in Codex as `mcp__kaoyan_math_read__math_read_bundle`; use that fully qualified tool for every chunk. Global namespaces `mcp__kaoyan_read__math_read_bundle`, `mcp__kaoyan_read_v2__math_read_bundle`, and `mcp__kaoyan_read_v3__math_read_bundle` are legacy and must not be called. The 408 and English namespaces are cross-subject and must not be called. Current-question evidence, warmup, visual detail, source images, Wiki/backlinks, Chronicle, HTML generation/rendering, capture, and writes remain on their existing routes and are never made eligible by this section.

Every MCP request must use `schema_version=study-read-mcp.v3` and route context for caller `kaoyan-math-visual-review`, Skill version `2.1.0`, the installed plugin version, a unique route request ID, one canonical evidence-scope hash, ordered chunk metadata, and `consumed_duplicate_read_count=0`. Request no more than 24 formal IDs per chunk. Larger scopes are split deterministically; all chunks must bind the same generation and authority fingerprint. One failed or mismatched chunk invalidates the entire route.

Before the first eligible call, check the callable task snapshot for the exact required tool `mcp__kaoyan_math_read__math_read_bundle`. If that exact tool is absent, legacy or cross-subject namespaces do not satisfy the requirement: call no MCP tool, record `read_route=terminal_fallback`, `required_mcp_tool=mcp__kaoyan_math_read__math_read_bundle`, `fallback_reason=tool_snapshot_missing`, `chunk_index=0`, and `chunk_count=0`, then immediately run the whole original Evidence Route without probing, retrying, or restarting Codex. For any other unavailable, incomplete, over-limit, inconsistent, drift, mismatch, or timeout response from the required math tool, discard every MCP chunk before rebinding the complete scope to one fresh canonical terminal generation. Never combine MCP and fallback evidence.

After MCP success, consumed terminal duplicate reads for the covered cold evidence must be zero. Terminal reads of non-covered sources are allowed and must be labeled terminal-only. Treat projections and candidates as non-formal, require `formal_write_count=0` and `model_call_count=0`, and never use this route for a current question or to authorize a write.
