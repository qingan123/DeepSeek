# 故障排查

## Telegram完全不回复

```bash
systemctl --user is-active hermes-gateway.service
systemctl is-active baidu-openai-proxy-60089.service
curl -sS http://127.0.0.1:60089/v1/health
journalctl --user -u hermes-gateway.service --since '-10 min' --no-pager
journalctl -u baidu-openai-proxy-60089.service --since '-10 min' --no-pager
```

先确认消息是否到Hermes，再看Hermes是否调用60089，最后看DeepSeek上游。

## 网页端很久才看到消息

比较日志中的 `create_pow_challenge` 和 `chat/completion`。正常原生PoW通常相差不到1秒。

```bash
ls -l scripts/deepseek_pow/dspow_native
bash scripts/deepseek_pow/build_native.sh
journalctl -u baidu-openai-proxy-60089.service | grep -Ei 'native PoW|falling back'
```

不要通过缓存PoW解决。

## HTTP 200但正文为空

常见原因：复用已使用的PoW、网页会话失效、userToken失效或上游SSE变化。禁用PoW缓存/预热，检测凭证，重置对应会话，再查本轮日志。

## DSML原样发到Telegram

确认：

```text
tool_client_profile=hermes
tool_call_mode=force_buffer
```

核对服务器 `app/api/openai.py` 并执行 `scripts/test_hermes_tool_compat.py`。

## 工具执行成功但模型说没输出

检查下一轮请求是否真的携带：

- Chat Completions：`role=tool`、正确 `tool_call_id`、非空content；
- Responses：`function_call_output`或对应output类型。

不要只看模型文字，要查后台会话记录和实际请求体。

## 图片消息卡住

```bash
ls -lt /root/.hermes/cache/images/ | head
journalctl --user -u hermes-gateway.service --since '-10 min' --no-pager | grep -Ei 'image|vision|zhipu'
```

60089文本接口不应作为图片视觉后端。

## 凭证失败

浏览器重新登录DeepSeek，复制新 `userToken`，更新后台凭证并检测，然后重置失效会话。

## 只有1核

不会部署失败。线程数按 `os.availableParallelism()`降低：

```bash
taskset -c 0 node -e "const os=require('os'); console.log(os.availableParallelism())"
```

应输出1。

## 服务启动失败

```bash
systemctl status baidu-openai-proxy-60089.service --no-pager
journalctl -u baidu-openai-proxy-60089.service -n 100 --no-pager
python -m py_compile app/main.py app/api/openai.py app/adapters/deepseek.py
```

检查工作目录、虚拟环境路径、`.env`权限、端口占用和数据库权限。
## 酒馆角色卡首轮回复通用拒绝

当酒馆长角色卡首轮偶发收到 `Sorry, that's beyond my current scope. Let's talk about something else.` 时，这是 DeepSeek 网页端对超长系统提示的偶发通用拒绝，不是角色卡语法错误。60089 已默认启用 `deepseek_generic_refusal_retry=true`：先暂存可能的拒绝前缀，命中后清空绑定窗口，用同一原始请求自动重试一次；二次仍失败时只返回中性故障提示，不把拒绝文本写入剧情上下文。

检查设置与日志：

```bash
sqlite3 data/app.db "select key,value from app_settings where key='deepseek_generic_refusal_retry';"
journalctl -u baidu-openai-proxy-60089.service --since '-30 min' --no-pager | grep -Ei 'generic upstream refusal|reset DeepSeek session|retry once'
```

不要删除或精简酒馆角色卡的系统提示；先确认是否命中该自动恢复流程。

## 酒馆 XML 被记录为 `unparsed tool-like output`

若正常回复以 `<chat_room>`、`<horae>`、`<messages>` 等标签排版，它们属于角色卡正文，不是工具调用。当前版本只识别明确的 DSML、OpenAI `tool_calls` JSON 和 function/invoke 结构；普通 XML 应原样下发。

回归检查：

```bash
PYTHONPATH=. venv/bin/python scripts/test_parse_tool.py
PYTHONPATH=. venv/bin/python scripts/test_hermes_tool_compat.py
```

预期酒馆 XML 判定为 `False`/零工具调用，Hermes 测试 `failures` 为空。

