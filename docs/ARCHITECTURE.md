# 架构说明

## 组件

```text
客户端/Hermes
  │ OpenAI协议
  ▼
FastAPI app/main.py
  ├─ app/api/openai.py      请求规范化、工具调用、流式输出
  ├─ app/services/          鉴权、配额、日志
  ├─ app/db/                SQLite设置、凭证、会话和审计
  ├─ app/web/               管理后台
  └─ app/adapters/deepseek.py
       ├─ 凭证选择
       ├─ HIF请求头
       ├─ PoW challenge与求解
       ├─ DeepSeek网页会话
       └─ SSE解析
```

## OpenAI兼容层

主要端点：`GET /v1/models`、`POST /v1/chat/completions`、`POST /v1/responses`、`POST /v1/files`、`POST /v1/images/generations`、`GET /v1/health`。

`app/api/openai.py`把OpenAI消息、工具Schema和历史结果转换成DeepSeek可理解的网页prompt，并将上游正文、reasoning和工具调用重新封装成OpenAI格式。

## 会话

- `stateless`：依赖客户端消息历史；
- `bound`：保存客户端会话到DeepSeek `chat_session_id`的绑定；
- `hybrid`：有客户端会话标识时绑定，否则无状态。

DeepSeek网页接口每次提交一条prompt，多轮由 `parent_message_id` 关联。

## PoW

```text
Python adapter
  → node solve_pow.js
      → dspow_native（首选）
      → 官方JS worker自适应多核兜底
```

Node包装器负责解析challenge、按实际CPU选线程、调用原生程序、失败回退和输出固定JSON。

## 工具调用

网页模型只能输出文本，因此60089通过提示规范让模型输出DSML/JSON，再解析成标准OpenAI `tool_calls`。客户端执行工具后，把 `role=tool` 或 Responses `function_call_output` 回传。

不能删除完整工具Schema。可优化历史和传输，但不能靠猜测补全工具参数。

## Telegram图片

Telegram图片先由Hermes下载。自动视觉预处理使用 `hermes_patches/tools/vision_tools.py` 对应的智谱视觉桥接；60089当前DeepSeek网页主链路仍是文本接口。