# Project 60089 当前统一交接文档

> 基线日期：2026-08-16  
> 本文是当前唯一主交接文档。旧60088分析、阶段报告和源码备份已移入 `archive/`。  
> 文档不记录 API Key、DeepSeek userToken、Cookie、Telegram Token 或 SSH 密码。

## 1. 项目目标

对外提供 OpenAI-compatible API，对内使用 DeepSeek 网页端协议完成对话，重点兼容 Hermes Agent 的工具调用、工具结果回传和 Telegram 入口。

## 2. 当前生产环境

```text
生产主机：192.168.1.9
项目目录：/root/work/baidu_ds_zip/project_60089
本地源码：D:\codex工作区\deep网页端\project_60089_code\project_60089
API监听：http://127.0.0.1:60089/v1
管理后台：http://127.0.0.1:60089/admin
systemd：baidu-openai-proxy-60089.service（历史名称，暂不改名）
Hermes目录：/root/.hermes/hermes-agent
Hermes user service：hermes-gateway.service
```

60089 默认只监听回环地址，由 Hermes 本机访问。若要对局域网或公网开放，优先用 Nginx/Caddy 反向代理并加 TLS、鉴权和访问控制。

## 3. 请求链路

```text
Telegram → Hermes Gateway
  → /v1/responses 或 /v1/chat/completions
  → 60089整理消息、完整tools schema和会话信息
  → 获取HIF头与本轮PoW challenge
  → dspow_native原生求解（失败则官方JS兜底）
  → DeepSeek chat_session/create + chat/completion
  → 解析网页SSE并转换为正文/reasoning/tool_calls
  → Hermes执行工具并回传tool结果
```

## 4. 核心文件

| 文件 | 作用 |
|---|---|
| `app/main.py` | FastAPI入口与路由注册 |
| `app/api/openai.py` | OpenAI协议、Responses、工具转换、SSE心跳 |
| `app/adapters/deepseek.py` | 凭证、HIF、会话、PoW和网页SSE |
| `app/db/init_db.py` | 默认模型、系统设置和数据库初始化 |
| `app/web/admin.py` | 管理后台、凭证检测、API Key管理 |
| `scripts/deepseek_pow/solve_pow.js` | 原生优先、JS回退和线程数选择 |
| `scripts/deepseek_pow/dspow_native.c` | DeepSeekHashV1自定义23轮Keccak |
| `scripts/deepseek_pow/build_native.sh` | 在目标Linux CPU上编译原生程序 |
| `scripts/test_hermes_tool_compat.py` | Hermes工具协议回归测试 |
| `hermes_patches/tools/vision_tools.py` | Telegram图片智谱视觉补丁副本 |

## 5. PoW实现与性能

```text
prefix = salt + "_" + expireAt + "_"
从answer=0开始搜索：
custom_keccak_23(prefix + answer) == challenge
```

它是官方网页 worker 的自定义23轮 Keccak，不等于标准 SHA3-256。

求解优先级：

1. `dspow_native`：C、pthread、多核搜索；
2. 原生程序缺失或失败：官方 JS worker；
3. `POW_NATIVE_REQUIRED=1` 时原生失败直接报错，仅用于排查。

线程数不是硬编码4核：

```text
workers = min(POW_WORKERS默认4, os.availableParallelism(), difficulty)
```

- 1核服务器自动1线程；
- 2核自动2线程；
- 4核及以上默认最多4线程；
- CPU affinity/cgroup限制也由 `os.availableParallelism()`识别；
- 已用 `taskset -c 0`模拟1核，最终选择1线程。

N3540验证：

```text
官方JS四核：12690 ms
原生C四核：   47 ms
同题answer：完全一致
完整API：2.445秒、2.091秒，HTTP 200且正文非空
```

禁止跨请求复用PoW、后台预热或多消息共享 `x-ds-pow-response`。真实验证表明复用可能得到 HTTP 200 但 SSE 正文为空。

## 6. Hermes兼容状态

当前支持 Chat Completions与Responses工具调用、DSML/JSON解析、工具名别名、参数格式转换、长工具调用心跳和真实工具结果回传。不能删除或精简 Hermes 完整工具Schema。

生产推荐设置：

```text
tool_client_profile=hermes
tool_call_mode=force_buffer
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

## 7. 新服务器部署

```bash
cd /opt/project_60089
sudo bash scripts/install.sh --port 60089 --admin-password '强密码'
curl -sS http://127.0.0.1:60089/v1/health
```

随后登录后台添加 DeepSeek `userToken`、检测凭证并创建本地 API Key。完整步骤见 `docs/DEPLOYMENT.md`。

## 8. 日常命令

```bash
systemctl status baidu-openai-proxy-60089.service --no-pager
systemctl restart baidu-openai-proxy-60089.service
journalctl -u baidu-openai-proxy-60089.service -f
curl -sS http://127.0.0.1:60089/v1/health

systemctl --user status hermes-gateway.service --no-pager
journalctl --user -u hermes-gateway.service -f
```

重编译PoW：

```bash
cd /root/work/baidu_ds_zip/project_60089/scripts/deepseek_pow
./build_native.sh
systemctl restart baidu-openai-proxy-60089.service
```

## 9. 更新与回滚

更新前备份 `data/app.db` 和当前源码。顺序：同步源码 → 安装依赖 → 编译PoW → `py_compile` → 回归测试 → 重启60089 → 健康检查。

不要在 Hermes 有业务执行时随意重启 Hermes；60089 和 Hermes 是两个独立服务。

### 9.1 DeepSeek角色卡通用拒绝恢复

当酒馆长角色卡首轮偶发收到 `Sorry, that's beyond my current scope. Let's talk about something else.` 时，60089 会在 `app/api/openai.py` 的 Chat Completions/Responses 流式与非流式路径识别该拒绝，暂不下发，清空绑定窗口并使用同一请求自动重试一次。设置项 `deepseek_generic_refusal_retry=true` 已写入 `data/app.db`；不要删减角色卡内容来规避此问题。部署后只需重启 `baidu-openai-proxy-60089.service`，无需重启 Hermes。

## 10. 安全要求

- `.env`、数据库、日志和任何Token不进入发布包；
- 新机器必须修改 `APP_SECRET` 和后台密码；
- 管理后台不要直接暴露公网；
- API Key应限制模型、额度和来源IP；
- 日志正文只在短期排障时开启；
- 曾写入旧脚本或聊天记录的Token应视为已经暴露并更换。

## 11. 文档规则

以 `README.md`、本文件和 `docs/` 为准。`archive/`只用于追溯，不参与部署，也不能作为当前操作依据。
