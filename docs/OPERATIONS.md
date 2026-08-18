# 运维手册

## 服务管理

当前 `.9` 生产机使用历史服务名 `baidu-openai-proxy-60089.service`。新安装脚本默认生成 `deepseek-web-proxy-60089.service`；执行命令前先确认实际名称。

```bash
systemctl status baidu-openai-proxy-60089.service --no-pager
systemctl restart baidu-openai-proxy-60089.service
systemctl enable baidu-openai-proxy-60089.service
```

Hermes是user service：

```bash
systemctl --user status hermes-gateway.service --no-pager
systemctl --user restart hermes-gateway.service
```

不要把两个服务混在一起；业务运行时只重启确实故障的一层。

## 健康检查

```bash
curl -sS http://127.0.0.1:60089/v1/health
ss -lntp | grep 60089
ps -ef | grep -E 'uvicorn|hermes' | grep -v grep
```

## 日志

```bash
journalctl -u baidu-openai-proxy-60089.service -f
journalctl -u baidu-openai-proxy-60089.service --since '-30 min' --no-pager
journalctl --user -u hermes-gateway.service -f
tail -f /root/.hermes/logs/agent.log
```

PoW性能看 `create_pow_challenge` 到 `chat/completion` 的时间差。原生正常时通常不到1秒；数秒到几十秒说明可能回退JS。

## PoW维护

```bash
cd /root/work/baidu_ds_zip/project_60089/scripts/deepseek_pow
./build_native.sh
ls -l dspow_native
```

环境变量必须由systemd或启动shell提供：

```text
POW_WORKERS=4          默认最多4线程，不超过可用CPU
POW_DISABLE_NATIVE=1   强制使用JS，仅排查时使用
POW_NATIVE_REQUIRED=1  原生失败直接报错
POW_NATIVE_BIN=/path   指定其他原生二进制
```

不要随意把这些变量塞进 `.env`；生产systemd可使用单独的 `Environment=` 行。

## 数据备份

```bash
systemctl stop baidu-openai-proxy-60089.service
cp -a data/app.db "data/app.db.backup.$(date +%Y%m%d-%H%M%S)"
systemctl start baidu-openai-proxy-60089.service
```

`.env`含敏感配置，应加密保存，不进入源码包。

## 回滚

1. 停止60089；
2. 恢复源码快照；
3. 如有数据库迁移，恢复对应数据库；
4. 重编译PoW；
5. 执行编译和回归测试；
6. 启动并检查日志。

```bash
python -m py_compile app/main.py app/api/openai.py app/adapters/deepseek.py
python scripts/test_hermes_tool_compat.py
bash scripts/deepseek_pow/build_native.sh
systemctl restart baidu-openai-proxy-60089.service
```

## 发布包

```bash
python scripts/package_release.py
```

发布包不得包含 `.env`、数据库、日志、Token、Cookie、`archive/` 和旧机器的 `dspow_native`。
