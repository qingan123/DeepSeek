#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/qingan123/DeepSeek.git}"
APP_DIR="${APP_DIR:-/opt/deepseek-web-proxy}"
DEFAULT_PORT="${DEEPSEEK_PORT:-60089}"
SERVICE_PREFIX="deepseek-web-proxy"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || fail "缺少命令: $1。"; }
read_tty() { local prompt="$1" var; IFS= read -r -p "$prompt" var </dev/tty || fail '无法读取终端输入'; printf '%s' "$var"; }
read_secret() { local prompt="$1" var; IFS= read -r -s -p "$prompt" var </dev/tty || fail '无法读取终端密码'; printf '\n' >/dev/tty; printf '%s' "$var"; }

[[ "${EUID}" -eq 0 ]] || fail '请使用 root 或 sudo 执行此脚本。'
need_cmd git
need_cmd python3
need_cmd curl
need_cmd node
need_cmd gcc
need_cmd systemctl

port="$(read_tty "服务端口 [${DEFAULT_PORT}]: ")"; port="${port:-$DEFAULT_PORT}"
[[ "$port" =~ ^[0-9]+$ && "$port" -ge 1 && "$port" -le 65535 ]] || fail '端口必须是 1-65535 的数字。'
password="$(read_secret '后台管理员密码（至少 8 位）: ')"
[[ "${#password}" -ge 8 ]] || fail '管理员密码至少需要 8 位。'
confirm="$(read_secret '确认管理员密码: ')"
[[ "$password" == "$confirm" ]] || fail '两次密码不一致。'
unset confirm
service_name="${SERVICE_PREFIX}-${port}"

if [[ -e "$APP_DIR/.git" ]]; then
  git -C "$APP_DIR" fetch --depth 1 origin main
  git -C "$APP_DIR" reset --hard origin/main
elif [[ -e "$APP_DIR" ]]; then
  fail "$APP_DIR 已存在但不是 Git 仓库，请备份后移走。"
else
  git clone --depth 1 --branch main "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"
[[ -f .env.example && -f scripts/deepseek_pow/build_native.sh ]] || fail '仓库缺少部署文件。'
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
mkdir -p data logs
chmod 700 data logs
[[ -f .env ]] || cp .env.example .env
INSTALL_PORT="$port" INSTALL_ADMIN_PASSWORD="$password" .venv/bin/python - .env <<'PY'
import os, secrets, sys
from pathlib import Path
path = Path(sys.argv[1])
lines = path.read_text(encoding='utf-8').splitlines()
values = {line.split('=', 1)[0]: line.split('=', 1)[1] for line in lines if '=' in line and not line.lstrip().startswith('#')}
values['APP_HOST'] = '127.0.0.1'
values['APP_PORT'] = os.environ['INSTALL_PORT']
values['ADMIN_PASSWORD'] = os.environ['INSTALL_ADMIN_PASSWORD']
if not values.get('APP_SECRET') or values['APP_SECRET'] == 'change-me':
    values['APP_SECRET'] = secrets.token_urlsafe(48)
order = list(values)
path.write_text(''.join(f'{key}={values[key]}\n' for key in order), encoding='utf-8')
PY
chmod 600 .env
bash scripts/deepseek_pow/build_native.sh
INSTALL_ADMIN_PASSWORD="$password" .venv/bin/python - <<'PY'
import os
from app.db.init_db import init_db
init_db(admin_password=os.environ['INSTALL_ADMIN_PASSWORD'])
PY
unset password INSTALL_PORT INSTALL_ADMIN_PASSWORD

cat > "/etc/systemd/system/${service_name}.service" <<EOF
[Unit]
Description=DeepSeek Web OpenAI-Compatible Proxy (${port})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
Environment="PATH=${APP_DIR}/.venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=${APP_DIR}/.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port ${port}
Restart=always
RestartSec=3
TimeoutStopSec=30
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now "$service_name"
healthy=false
for _ in $(seq 1 30); do
  if curl -fsS --max-time 2 "http://127.0.0.1:${port}/v1/health" >/dev/null 2>&1; then healthy=true; break; fi
  sleep 1
done
[[ "$healthy" == true ]] || { systemctl status "$service_name" --no-pager -l || true; fail "服务未通过健康检查，请运行: journalctl -u $service_name -n 100 --no-pager"; }

printf '\n部署完成。\n后台账户: admin\n后台地址: http://服务器IP:%s/admin\n项目目录: %s\n服务名: %s\n' "$port" "$APP_DIR" "$service_name"
