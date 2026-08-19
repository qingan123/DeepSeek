## 一键部署与更新

在 Debian/Ubuntu 服务器上直接执行，脚本会自动安装 Git、Python、Node.js、GCC 等依赖，询问端口和管理员密码，创建 systemd 服务并检查健康状态：

```bash
sudo bash -c 'curl -fsSL https://raw.githubusercontent.com/qingan123/DeepSeek/main/scripts/one-click-install.sh | bash'
```

后续更新（更新前会检查未提交修改并保留 `.env`）：

```bash
sudo bash -c 'curl -fsSL https://raw.githubusercontent.com/qingan123/DeepSeek/main/scripts/update.sh | bash'
```

后台账户名为 `admin`，地址为 `http://服务器IP:你选择的端口/admin`。以下为项目原始说明。

# DeepSeek Web OpenAI-Compatible Proxy (60089)

把 **DeepSeek 网页端**封装成 OpenAI-compatible API，供 Hermes、Telegram Bot、OpenAI SDK 及其他客户端使用。当前生产实例监听 `127.0.0.1:60089`，上游是 `chat.deepseek.com` 网页协议，不是 DeepSeek 官方 API。

## 当前状态

- Chat Completions、Responses API：支持流式/非流式。
- Hermes 工具调用：支持 DSML/JSON 解析、工具结果回填、长调用心跳。
- DeepSeek 网页会话：支持凭证池、会话绑定、模型映射和文件上传。
- PoW：原生 C 自适应多核求解，失败时回退官方 JavaScript 算法。
- N3540 完整请求实测约 `2.1～2.5 秒`，实际速度仍受网络和模型生成影响。

## Linux 一键部署（推荐）

在一台全新的 Debian/Ubuntu Linux 服务器上直接复制下面一行。脚本会自动安装 Git、Python、Node.js、GCC 等依赖，然后交互询问服务端口、后台管理员密码和确认密码；密码不会出现在命令行参数或 GitHub 仓库中。脚本会自动克隆/更新仓库、安装 Python 依赖、编译原生 PoW、创建 systemd 服务并等待健康检查通过：

```bash
sudo bash -c 'curl -fsSL https://raw.githubusercontent.com/qingan123/DeepSeek/main/scripts/one-click-install.sh | bash'
```

部署完成后，后台账户名为 `admin`，地址为 `http://服务器IP:你选择的端口/admin`。首次登录后请立即修改管理员密码。脚本默认部署到 `/opt/deepseek-web-proxy`，默认端口为 `60089`，服务名按端口生成，例如 `deepseek-web-proxy-60089`。

查看服务状态：

```bash
sudo systemctl status deepseek-web-proxy-60089 --no-pager
curl -fsS http://127.0.0.1:60089/v1/health
```

然后在后台“凭证池”添加 DeepSeek 网页端 `userToken` 并检测，再在“API Key”页面创建客户端调用密钥。完整步骤见 [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)。

## 五分钟部署

```bash
cd project_60089
sudo bash scripts/install.sh --port 60089 --admin-password '换成强密码'
```

然后打开 `http://服务器IP:60089/admin`，在“凭证池”添加 DeepSeek 网页端 `userToken` 并检测，再在“API Key”页面创建客户端调用密钥。完整步骤见 [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)。

## API 示例

```bash
curl http://127.0.0.1:60089/v1/chat/completions \
  -H 'Authorization: Bearer YOUR_LOCAL_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-v4","messages":[{"role":"user","content":"只回复连接正常"}],"stream":false}'
```

## 目录

```text
project_60089/
├─ app/                       FastAPI主程序
├─ scripts/deepseek_pow/      原生C PoW与官方JS兜底
├─ deployment/                systemd模板
├─ hermes_patches/            Hermes侧补丁副本
├─ docs/                      当前有效文档
├─ archive/                   历史资料，不参与部署
├─ data/app.db                运行数据库，不进入发布包
├─ logs/                      运行日志，不进入发布包
├─ .env                       本机配置，不进入发布包
├─ .env.example               新机器配置模板
├─ HANDOFF.md                 当前统一交接文档
└─ README.md                  项目入口
```

## 文档入口

- [HANDOFF.md](HANDOFF.md)：当前生产状态和完整交接。
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)：新服务器部署、升级、迁移。
- [docs/OPERATIONS.md](docs/OPERATIONS.md)：服务、日志、备份、回滚。
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)：请求链路和模块职责。
- [docs/HERMES_COMPATIBILITY.md](docs/HERMES_COMPATIBILITY.md)：Hermes工具与图片链路。
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)：故障排查。
- [docs/CHANGELOG.md](docs/CHANGELOG.md)：关键修改记录。

## 必须知道

1. DeepSeek 网页端凭证会失效，需要在后台更新 `userToken`。
2. 每条消息都必须计算新的 PoW；禁止跨请求复用或后台预热。
3. `dspow_native` 是目标机器本地编译产物，发布包不携带旧机器二进制。
4. Windows 没有原生二进制时会自动使用 JS 兜底，速度会慢。
5. `.env`、数据库、日志、Token、Cookie 和 Telegram Token 禁止放进发布包。

## 生成干净发布包

```bash
python scripts/package_release.py
```

输出到 `dist/`，自动排除运行数据、归档、缓存和本机编译产物。