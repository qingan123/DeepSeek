#!/usr/bin/env python3
"""精确测试 _parse_tool_calls 三种格式"""
import sys
sys.path.insert(0, "/root/work/baidu_ds_zip/project_60089")
from app.api.openai import (
    _clean_roleplay_structured_output,
    _feed_roleplay_stream,
    _maybe_tool_call_markup,
    _normalize_sillytavern_markup,
    _normalize_tool_markup,
    _parse_tool_calls,
    _strip_roleplay_prefix,
)

tools = [{"type": "function", "function": {"name": "bash"}}]

# 1. 标准 OpenAI 对象格式(真实场景, 字符串含转义引号)
obj = '{"tool_calls": [{"id":"x","type":"function","function":{"name":"bash","arguments":"{\\"command\\":\\"pwd\\"}"}}]}'
print("1. 对象格式:", _parse_tool_calls(obj, tools=tools))

# 2. 纯 JSON 数组(图片场景)
arr = '[{"id":"c1","type":"function","function":{"name":"bash","arguments":{"command":"ls"}}}]'
print("2. 数组格式:", _parse_tool_calls(arr, tools=tools))

# 3. DSML 格式(原有)
dsml = '<|DSML| tool_calls><|DSML| invoke name="bash"><|DSML| parameter name="command">ls -la</|DSML| parameter></|DSML| invoke></|DSML| tool_calls>'
print("3. DSML 格式:", _parse_tool_calls(dsml, tools=tools))

# 4. 检查 normalize 是否破坏转义
n = _normalize_tool_markup(obj)
print("4. normalize 后:", n)

# 5. SillyTavern role-play XML is ordinary assistant content, not a tool call.
roleplay_xml = """<chat_room>
<header>[群名|我们的小窝]</header>
<messages>[群消息|清泠|文本|早上好]</messages>
</chat_room>
<horae>time:2026/08/18 07:26</horae>"""
assert not _maybe_tool_call_markup(roleplay_xml), "role-play XML was misclassified as tool markup"
assert _parse_tool_calls(roleplay_xml, tools=tools) == [], "role-play XML parsed as a tool call"

# Explicit DSML and OpenAI JSON must still enter tool parsing.
assert _maybe_tool_call_markup(dsml)
assert _maybe_tool_call_markup(obj)
assert _maybe_tool_call_markup(arr)
print("5. 酒馆 XML 放行，DSML/JSON 工具格式仍可识别")

broken_roleplay = """[群消息|暖雪|文本|第一条正确]
[群消息|暖雪|文本]第二条缺少内容分隔符]
[群消息|暖雪|文本]第三条也缺少分隔符]"""
fixed_roleplay = _normalize_sillytavern_markup(broken_roleplay)
assert "[群消息|暖雪|文本|第一条正确]" in fixed_roleplay
assert "[群消息|暖雪|文本|第二条缺少内容分隔符]" in fixed_roleplay
assert "[群消息|暖雪|文本|第三条也缺少分隔符]" in fixed_roleplay
assert "undefined" not in fixed_roleplay
print("6. 酒馆群消息缺失分隔符自动修复")

mixed = "```html\\n普通小说前言\\n\\n<chat_room>\\n<header>\\n[群名|测试]\\n</header>\\n</chat_room>\\n```"
assert _strip_roleplay_prefix(mixed).startswith("<chat_room>")
cleaned_mixed = _clean_roleplay_structured_output(mixed)
assert cleaned_mixed.startswith("<chat_room>")
assert not cleaned_mixed.endswith("```")
state = {"buffer": "", "decided": False, "structured": False, "enabled": True}
parts = [_feed_roleplay_stream(state, part) for part in ("```html\\n普通", "正文\\n", "<chat_", "room>\\n内容", "</chat_room>\\n```")]
parts.append(_feed_roleplay_stream(state, "", final=True))
stream_fixed = "".join(parts)
assert stream_fixed.startswith("<chat_room>")
assert "普通正文" not in stream_fixed
assert "```" not in stream_fixed
print("7. 混合正文+结构化区块时只下发结构化内容")
