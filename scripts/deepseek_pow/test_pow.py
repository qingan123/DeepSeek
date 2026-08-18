#!/usr/bin/env python3
"""Fetch one real challenge and benchmark the current PoW wrapper.

Provide credentials only through DEEPSEEK_USER_TOKEN. The script never stores or prints it.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOLVER = Path(__file__).resolve().with_name("solve_pow.js")
TOKEN = os.environ.get("DEEPSEEK_USER_TOKEN", "").strip()
BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://chat.deepseek.com").rstrip("/")

if not TOKEN:
    raise SystemExit("Set DEEPSEEK_USER_TOKEN before running this test.")

headers = {
    "x-client-bundle-id": "com.deepseek.chat",
    "x-client-platform": "web",
    "x-client-version": "2.3.0",
    "x-client-locale": "en_US",
    "x-client-timezone-offset": "28800",
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
}

with httpx.Client(timeout=30) as client:
    response = client.post(
        f"{BASE_URL}/api/v0/chat/create_pow_challenge",
        headers=headers,
        json={"target_path": "/api/v0/chat/completion"},
    )
    response.raise_for_status()
    challenge = response.json()["data"]["biz_data"]["challenge"]

started = time.perf_counter()
result = subprocess.run(
    ["node", str(SOLVER), json.dumps(challenge, separators=(",", ":"))],
    capture_output=True,
    text=True,
    timeout=180,
    cwd=PROJECT_ROOT,
)
elapsed = time.perf_counter() - started

if result.returncode != 0:
    print(result.stderr.strip(), file=sys.stderr)
    raise SystemExit(result.returncode)

answer = json.loads(result.stdout.strip())
print(json.dumps({"elapsed_seconds": round(elapsed, 3), "answer": answer}, ensure_ascii=False))