# 三科学习入库系统：方案与源码快照

这是数学、408、英语共享入库系统的一份历史工程快照，用来对照方案、理解跨项目关系和定位当时尚未完成的工作。它不作为当前生产部署来源。

## 内容

- `backend/`：共享后端的当时版本。
- `shared-mcp/`：只读学习 MCP 的当时版本。
- `subjects/`：学科侧的对接代码与合同。
- `handoff/`：方案、工程状态与交接说明。
- `SNAPSHOT_MANIFEST.md`：快照来源与范围。

它与独立的 [后端仓库](https://github.com/xialovezhu-wq/study-intake-v2-backend) 和 [MCP 仓库](https://github.com/xialovezhu-wq/local-study-read-mcp) 有明确区别：这里保留历史横截面，独立仓库保存各项目实现。

## 阅读与运行

先读 [快照说明](README.technical.md) 和 [来源清单](SNAPSHOT_MANIFEST.md)。按各子项目说明配置隔离环境；整份快照没有统一的一键启动入口。历史 `PARTIAL`、测试失败、未部署等结论照实保留，公开不将其改写为已验收完成。

只公开工程代码与方案，不包含原项目完整 Git 历史、真实学习库、对话、健康资料或运行凭据。历史文档中关于 Private 的叙述描述当时用途；本仓库已按所有者本次指令公开。
