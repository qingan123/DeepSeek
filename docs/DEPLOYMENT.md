# 部署与迁移指南

## 1. 推荐环境

Linux生产环境：

- 64位little-endian Linux；
- Python 3.11+；
- Node.js 18+；
- `gcc`、glibc、pthread，用于编译原生PoW；
- systemd；
- 能访问 `chat.deepseek.com`、`hif-leim.deepseek.com`、`hif-dliq.deepseek.com`；
- 至少1个CPU核心和512MB可用内存，建议2核以上。

只有1核也能部署，线程数自动降为1。缺少gcc仍可运行，但会回退JS，弱CPU延迟明显增加。

## 2. 准备发布包

```bash
python scripts/package_release.py
```

发布包自动排除 `.env`、数据库、日志、`archive/`、缓存和本机PoW二进制。

## 3. Linux首次部署

```bash
unzip project_60089-release-*.zip -d /opt/project_60089
cd /opt/project_60089
sudo bash scripts/install.sh --port 60089 --admin-password '替换为强密码'
```

默认生成的服务名是 `deepseek-web-proxy-60089.service`。当前 `.9` 生产机因为历史原因仍使用 `baidu-openai-proxy-60089.service`。

安装脚本将：

1. 创建 `.venv`；
2. 安装Python依赖；
3. 初始化 `.env` 和随机 `APP_SECRET`；
4. 初始化SQLite数据库和本地API Key；
5. 有gcc时编译 `dspow_native`；
6. 创建并启用systemd服务。

指定生产兼容服务名：

```bash
sudo bash scripts/install.sh \
  --port 60089 \
  --admin-password '强密码' \
  --service-name baidu-openai-proxy-60089
```

不安装systemd：

```bash
bash scripts/install.sh --port 60089 --admin-password '强密码' --no-service
```

## 4. 配置DeepSeek凭证

1. 打开 `/admin`；
2. 进入“凭证池”；
3. 新建凭证；
4. 内容填写DeepSeek网页端 `userToken`；
5. 启用并点击检测；
6. 确认状态为token有效。

不要把Token写进 `.env.example`、测试脚本或文档。

## 5. 配置客户端API Key

在后台“API Key”页面创建本地调用Key，并按需要限制模型、请求额度和来源IP。

```text
Hermes Base URL: http://127.0.0.1:60089/v1
Hermes API Key: 后台创建的本地Key
DeepSeek userToken: 只放在后台凭证池
```

## 6. Hermes推荐配置

```text
tool_client_profile=hermes
tool_call_mode=force_buffer
conversation_response_mode=client
tool_buffer_timeout_ms=180000
tool_max_buffer_chars=2000000
```

不能删除或精简Hermes发送的工具Schema。

## 7. 部署后验证

```bash
curl -sS http://127.0.0.1:60089/v1/health
systemctl is-active deepseek-web-proxy-60089.service
```

```bash
curl -sS http://127.0.0.1:60089/v1/models \
  -H 'Authorization: Bearer YOUR_LOCAL_API_KEY'
```

```bash
curl -sS http://127.0.0.1:60089/v1/chat/completions \
  -H 'Authorization: Bearer YOUR_LOCAL_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-v4","messages":[{"role":"user","content":"只回复部署成功"}],"stream":false}'
```

验证PoW二进制：

```bash
cd scripts/deepseek_pow
./build_native.sh
./dspow_native 2>&1 | head -1
```

输出usage说明代表二进制可以启动。

## 8. Windows部署

```powershell
.\scripts\install_windows.bat -Port 60089 -AdminPassword "强密码"
```

Windows默认没有Linux原生二进制，会自动回退JS。生产建议优先Linux；如自行编译Windows版本，可通过系统环境变量 `POW_NATIVE_BIN` 指定。

## 9. 迁移旧服务器

分开处理：

```text
源码：app/ scripts/ deployment/ docs/ requirements.txt
配置：.env
数据库：data/app.db
Hermes：/root/.hermes/（独立项目）
```

推荐顺序：

1. 新机完成干净安装；
2. 停止旧机60089写入；
3. 复制 `data/app.db` 和 `.env`；
4. 检查路径、端口和权限；
5. 在新机重新执行 `build_native.sh`，不要复制旧CPU编译的二进制；
6. 完成API测试；
7. 最后切换Hermes Base URL。

## 10. 升级现有实例

```bash
SERVICE=deepseek-web-proxy-60089.service
systemctl stop "$SERVICE"
cp -a data/app.db "data/app.db.backup.$(date +%Y%m%d-%H%M%S)"
# 同步新源码，但保留.env和data/app.db
source .venv/bin/activate
pip install -r requirements.txt
bash scripts/deepseek_pow/build_native.sh
python -m py_compile app/main.py app/api/openai.py app/adapters/deepseek.py
python scripts/test_hermes_tool_compat.py
systemctl start "$SERVICE"
curl -sS http://127.0.0.1:60089/v1/health
```
