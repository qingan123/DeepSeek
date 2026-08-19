#!/usr/bin/env bash
set -Eeuo pipefail
APP_DIR=${APP_DIR:-/opt/deepseek-web-proxy}; SERVICE=${SERVICE:-deepseek-web-proxy-60089.service}
cd "$APP_DIR"
[[ -z "$(git status --porcelain)" ]] || { echo '工作树有未提交修改，已停止更新。' >&2; exit 1; }
cp -a .env ".env.backup.$(date +%Y%m%d-%H%M%S)"
port=$(awk -F= '$1=="APP_PORT"{print $2}' .env); port=${port:-60089}
SERVICE=${SERVICE:-deepseek-web-proxy-$port.service}
git fetch --depth 1 origin main
git reset --hard origin/main
.venv/bin/python -m pip install -r requirements.txt
bash scripts/deepseek_pow/build_native.sh
systemctl restart "$SERVICE"
for _ in $(seq 1 30); do curl -fsS --max-time 2 "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && exit 0; sleep 1; done
systemctl status "$SERVICE" --no-pager -l; exit 1
