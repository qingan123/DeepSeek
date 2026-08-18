# Hermes兼容说明

## 当前连接

```text
Hermes Base URL：http://127.0.0.1:60089/v1
60089 systemd：baidu-openai-proxy-60089.service
Hermes user service：hermes-gateway.service
```

## 已解决问题

1. Responses `function_call_output`曾被转为空消息，现保留真实结果和 `tool_call_id`。
2. DSML曾作为普通文字泄漏到Telegram，现会缓冲并转换成OpenAI工具事件。
3. 大型工具参数超过60秒时会断流，现每15秒发送协议心跳。
4. `execute_command`、`shell_exec`、`bash`、`run_command`归一到 `terminal`。
5. camelCase参数会转换成snake_case，并兼容常见文件编辑参数。
6. 没有真实mutation成功结果时，模型不得声称文件已修改。

## 推荐后台配置

```text
tool_client_profile=hermes
tool_call_mode=force_buffer
conversation_response_mode=client
tokeny_tool_result_compaction=false
tokeny_compact_tool_schema_after_result=false
tool_buffer_timeout_ms=180000
tool_max_buffer_chars=2000000
tool_parse_retries=2
tool_loop_protection=true
tool_force_final_after_result=false
tool_repeat_protection_enabled=true
tool_repeat_protection_scope=write
tool_repeat_match_mode=smart
```

## 回归测试

```bash
cd /root/work/baidu_ds_zip/project_60089
./venv/bin/python scripts/test_hermes_tool_compat.py
```

新部署使用 `.venv` 时，把命令改为 `./.venv/bin/python`。

默认使用项目内置的基础工具fixture。要验证某台Hermes导出的完整Schema：

```bash
HERMES_TOOL_DEFS=/path/to/hermes-tools.json \
  ./.venv/bin/python scripts/test_hermes_tool_compat.py
```

必须验证Chat/Responses的stream与non-stream、首次工具调用、工具结果后的下一次工具调用、长参数心跳、DSML不泄漏和完整工具Schema。

## Telegram图片

Hermes补丁副本：`hermes_patches/tools/vision_tools.py`。生产对应文件：`/root/.hermes/hermes-agent/tools/vision_tools.py`。

图片流程：Telegram下载 → Hermes视觉预处理 → 智谱视觉 → 描述注入Agent上下文。视觉失败时应快速报错，不应回落到不支持图片的60089长期等待。

Hermes本体不在本项目目录。复制60089不会自动迁移Hermes配置、Telegram Token、MCP配置或Hermes源码补丁。
