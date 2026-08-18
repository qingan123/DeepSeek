# 关键变更记录

## 2026-08-18

### DeepSeek角色卡通用拒绝自动恢复

- 识别网页端偶发返回的通用拒绝短语（英文/中文及分段流式前缀）；
- 不把拒绝正文下发给酒馆或 Hermes，避免把拒绝写入角色剧情上下文；
- 自动清空当前 DeepSeek 窗口并用同一原始请求重试一次；
- Chat Completions、Responses 的非流式和流式路径统一处理；
- 新增设置 `deepseek_generic_refusal_retry=true`，默认开启；
- 二次仍失败时返回中性故障提示，并清除绑定，避免后续轮次继续复用坏窗口。
- 对连续拒绝的第二次重试增加等义文学创作兼容转写，不删减角色卡、剧情或工具参数。


## 2026-08-16

### 原生PoW加速

- 精确复现DeepSeekHashV1自定义23轮Keccak；
- 增加 `dspow_native.c` 和Linux编译脚本；
- 原生优先、官方JS自动回退；
- CPU线程数自适应，1核环境验证通过；
- N3540同题从12690ms降至47ms；
- 完整API连续测试约2.1～2.5秒。

### Hermes工具兼容

- 修复Responses工具结果丢失；
- 支持DSML/JSON工具调用解析；
- 增加工具别名、参数转换、重复写保护和SSE心跳；
- 保留完整工具Schema。

### Telegram图片

- Hermes视觉预处理改走智谱视觉桥接；
- 避免回落到不支持图片的60089文本接口。

### 项目整理

- 根目录改为 `README.md` + `HANDOFF.md` 单一入口；
- 当前文档集中到 `docs/`；
- 旧60088资料、阶段报告和备份移入 `archive/`；
- 一次性测试脚本归档并清除硬编码Token；
- 增加干净发布包工具。

### 酒馆 XML 与工具调用识别隔离

- 修复 `<chat_room>`、`<horae>`、`<messages>`、`<header>` 等酒馆角色扮演 XML 被误判为工具调用；
- 移除“任何以 `<` 开头的回复都是工具调用”的宽泛判断；
- 仅对明确的 DSML、`tool_calls` JSON、function/invoke 工具结构进入工具解析；
- 增加酒馆 XML 放行回归，同时保留 Hermes DSML/JSON/Responses 工具兼容。

