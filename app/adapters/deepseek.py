"""
DeepSeek 网页版适配器 (chat.deepseek.com)

把 DeepSeek 网页端能力封装为与 BaiduAdapter 相同的接口契约,
供 API 层 (openai.py) 无感切换上游。

关键机制:
1. 鉴权: Authorization: Bearer <userToken>(凭证存 credentials.cookie 字段)
2. PoW: 每次对话前调 create_pow_challenge, 由 Node 包装器调用原生C求解
   - 原生异常时自动退回官方 worker；禁止跨请求缓存或复用PoW结果
3. 会话: chat_session/create 拿 uuid; 多轮用 parent_message_id 关联
4. SSE: event: ready → 增量 data → event: close

接口契约(BaiduAdapter 同名):
- build_query(messages, force_prompt) -> str
- init_context(token_override=None, credential_id=None) -> DeepSeekContext
- get_model(public_id) -> ModelConfig
- stream_conversation(query, public_model, direct_answer, baidu_session_id,
                      rank, credential_id, cookie_override) -> AsyncIterator[ParsedEvent]
- parse_event_line(line) -> ParsedEvent | None
"""

import asyncio
import base64
import json
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import httpx
import orjson
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.init_db import get_setting
from app.db.models import BaiduConversation, Credential, ModelConfig, PromptConfig
from app.services.logging_service import system_log

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[2]
POW_SCRIPT = PROJECT_ROOT / "scripts" / "deepseek_pow" / "solve_pow.js"
POW_DIFFICULTY_LIMIT = 200000

# 与 baidu.py 保持一致的 ParsedEvent 契约
@dataclass
class ParsedEvent:
    component: str
    text: str = ""
    reasoning: str = ""
    images: list[str] | None = None
    workspace_file_id: str | None = None
    workspace_content_delta: str = ""
    raw: dict[str, Any] | None = None
    lid: str = ""
    qid: str = ""
    session_id: str = ""
    pkg_id: str = ""
    seq_id: int | None = None
    finished: bool = False


@dataclass
class DeepSeekContext:
    token: str
    hif_leim: str = ""
    hif_dliq: str = ""
    credential_id: int | None = None
    credential_name: str = "anonymous"


class DeepSeekAdapter:
    def __init__(self, db: Session):
        self.db = db
        self.settings = get_settings()
        # hif 缓存(类级, 进程内)
        self._hif_cache: dict[str, tuple[str, float]] = {}

    # ---------------------------------------------------------------- 基础
    def get_model(self, public_id: str) -> ModelConfig:
        model = self.db.scalar(
            select(ModelConfig).where(ModelConfig.public_id == public_id, ModelConfig.enabled.is_(True))
        )
        if not model:
            # 未知模型 fallback: 优先 smart(老配置), 其次任意启用模型, 避免 500
            model = self.db.scalar(select(ModelConfig).where(ModelConfig.public_id == "smart", ModelConfig.enabled.is_(True)))
        if not model:
            model = self.db.scalars(
                select(ModelConfig).where(ModelConfig.enabled.is_(True)).order_by(ModelConfig.id.asc()).limit(1)
            ).first()
        if not model:
            raise RuntimeError("No enabled model config found")
        return model

    def get_prompt_config(self) -> PromptConfig | None:
        return self.db.scalar(select(PromptConfig).where(PromptConfig.name == "default"))

    def build_query(self, messages: list[dict[str, Any]], force_prompt: bool = True) -> str:
        """与 BaiduAdapter 相同: messages -> 单段文本。DeepSeek 以单条 prompt 提交。"""
        parts: list[str] = []
        prompt = self.get_prompt_config()
        if force_prompt and prompt and prompt.enabled and prompt.content:
            parts.append(f"系统提示词：\n{prompt.content}")
        for item in messages:
            role = item.get("role", "user")
            content = item.get("content", "")
            if role == "tool":
                tool_name = item.get("name") or item.get("tool_call_id") or "tool"
                parts.append(f"工具 {tool_name} 的执行结果：\n{content}")
                continue
            if item.get("tool_calls"):
                parts.append(f"助手请求调用工具：\n{json.dumps(item.get('tool_calls'), ensure_ascii=False)}")
                continue
            if isinstance(content, list):
                content_text = "\n".join(str(part.get("text", part)) for part in content)
            else:
                content_text = str(content)
            if role == "system":
                parts.append(f"系统：{content_text}")
            elif role == "assistant":
                parts.append(f"助手：{content_text}")
            else:
                parts.append(f"用户：{content_text}")
        return "\n\n".join(parts).strip()

    # ---------------------------------------------------------------- 凭证/上下文
    async def init_context(
        self,
        token_override: str | None = None,
        credential_id: int | None = None,
    ) -> DeepSeekContext:
        """获取 DeepSeek 上下文: token + hif 头。凭证 = userToken(存 credentials.cookie 字段)。

        凭证选择规则:
        - token_override 优先(绑定会话的 cookie 快照)
        - credential_id 次之(绑定会话的固定凭证)
        - 否则自动: 按 failure_count asc, last_used_at asc 轮询, 失败自动跳过并标记
        """
        token = ""
        credential = None
        if token_override is not None:
            token = token_override
        elif credential_id is not None:
            credential = self.db.get(Credential, credential_id)
            if not credential or not credential.enabled:
                raise RuntimeError(f"Credential {credential_id} missing or disabled")
            token = credential.cookie
        else:
            mode = get_setting(self.db, "credential_mode", "auto").lower()
            if mode != "anonymous":
                creds = self.db.scalars(
                    select(Credential)
                    .where(Credential.enabled.is_(True))
                    .order_by(Credential.failure_count.asc(), Credential.last_used_at.asc(), Credential.id.asc())
                ).all()
                if creds:
                    # 自动轮询: 逐个尝试直到成功 (失败会被 _mark_credential_failed 标记并跳过)
                    last_error: Exception | None = None
                    for candidate in creds:
                        try:
                            hif_leim, hif_dliq = await self._fetch_hif_headers()
                            token = candidate.cookie
                            credential = candidate
                            break
                        except Exception as exc:
                            last_error = exc
                            self._mark_credential_failed(candidate, exc)
                            system_log(
                                self.db, "WARNING", "deepseek_credential",
                                f"credential failed id={candidate.id} name={candidate.name}: {str(exc)[:300]}",
                            )
                    if not token:
                        if mode == "pool":
                            raise RuntimeError(f"Credential mode is pool, but all enabled credentials failed: {last_error}")
                        # auto 模式全部失败 → 匿名尝试(无 token 会走 hif 空头)
                        if last_error is not None:
                            system_log(
                                self.db, "WARNING", "deepseek_credential",
                                f"all enabled credentials failed, last error: {str(last_error)[:300]}",
                            )
                # 无凭证时 auto 模式也允许匿名(DeepSeek 网页版可能拒绝, 由上游决定)

        if not token:
            raise RuntimeError("No DeepSeek token available")

        hif_leim, hif_dliq = await self._fetch_hif_headers()

        if credential:
            today = time.strftime("%Y-%m-%d")
            if credential.use_count_date != today:
                credential.use_count_date = today
                credential.use_count_today = 0
            credential.use_count_total += 1
            credential.use_count_today += 1
            credential.failure_count = 0
            credential.last_status = "ok"
            credential.last_used_at = __import__("datetime").datetime.utcnow()
            self.db.commit()

        return DeepSeekContext(
            token=token,
            hif_leim=hif_leim,
            hif_dliq=hif_dliq,
            credential_id=credential.id if credential else None,
            credential_name=credential.name if credential else "anonymous",
        )

    def _mark_credential_failed(self, credential: Credential, exc: Exception) -> None:
        """标记凭证失败: failure_count +1, 超阈值自动禁用"""
        try:
            credential.failure_count += 1
            credential.last_status = f"failed: {str(exc)[:52]}"
            threshold = self._credential_failure_threshold()
            if threshold > 0 and credential.failure_count >= threshold:
                credential.enabled = False
                credential.last_status = f"disabled after {credential.failure_count} failures: {str(exc)[:36]}"
            self.db.commit()
        except Exception:
            self.db.rollback()

    def _credential_failure_threshold(self) -> int:
        try:
            return max(0, int(get_setting(self.db, "credential_failure_disable_threshold", "3") or 3))
        except ValueError:
            return 3

    async def _fetch_hif_headers(self) -> tuple[str, str]:
        """拉取 x-hif-leim / x-hif-dliq(5 分钟缓存)"""
        now = time.time()
        if self._hif_cache.get("leim", (None, 0))[1] > now:
            return self._hif_cache["leim"][0], self._hif_cache["dliq"][0]

        async with httpx.AsyncClient(timeout=20) as client:
            try:
                r1 = await client.get("https://hif-leim.deepseek.com/query")
                r2 = await client.get("https://hif-dliq.deepseek.com/query")
                leim = (r1.json().get("data", {}).get("biz_data", {}).get("value") or "") if r1.status_code == 200 else ""
                dliq = (r2.json().get("data", {}).get("biz_data", {}).get("value") or "") if r2.status_code == 200 else ""
            except Exception as exc:
                system_log(self.db, "WARNING", "deepseek_hif", f"hif fetch failed: {str(exc)[:200]}")
                leim, dliq = "", ""

        if leim:
            self._hif_cache["leim"] = (leim, now + 300)
        if dliq:
            self._hif_cache["dliq"] = (dliq, now + 300)
        return leim, dliq

    # ---------------------------------------------------------------- PoW
    async def _get_pow_header(self, token: str, target_path: str = "/api/v0/chat/completion") -> str:
        """为本次上游请求获取并独立求解 x-ds-pow-response。"""
        challenge = await self._create_pow_challenge(token, target_path)
        answer = await self._solve_pow(challenge)
        pow_payload = {
            "algorithm": challenge.get("algorithm"),
            "challenge": challenge.get("challenge"),
            "salt": challenge.get("salt"),
            "answer": answer,
            "signature": challenge.get("signature"),
            "target_path": target_path,
        }
        return base64.b64encode(
            json.dumps(pow_payload, separators=(",", ":")).encode()
        ).decode()

    async def _create_pow_challenge(self, token: str, target_path: str) -> dict[str, Any]:
        headers = self._base_headers(token)
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{self.settings.deepseek_base_url}/api/v0/chat/create_pow_challenge",
                headers=headers,
                json={"target_path": target_path},
            )
            r.raise_for_status()
            j = r.json()
            if j.get("code") != 0:
                raise RuntimeError(f"create_pow_challenge failed: code={j.get('code')} msg={j.get('msg')}")
            return j["data"]["biz_data"]["challenge"]

    async def _solve_pow(self, challenge: dict[str, Any]) -> int:
        """Node 子进程调用官方 worker 求解"""
        if not POW_SCRIPT.exists():
            raise RuntimeError(f"PoW solver not found: {POW_SCRIPT}")
        # 补 expireAt(worker 解构需要)
        challenge = dict(challenge)
        if "expireAt" not in challenge and "expire_at" in challenge:
            challenge["expireAt"] = challenge["expire_at"]
        proc = await asyncio.create_subprocess_exec(
            "node", str(POW_SCRIPT), json.dumps(challenge, separators=(",", ":")),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError("PoW solve timeout")
        if proc.returncode != 0:
            raise RuntimeError(f"PoW solve failed: {stderr.decode(errors='replace')[:300]}")
        result = json.loads(stdout.decode(errors="replace").strip())
        answer = result.get("answer")
        if not isinstance(answer, int):
            raise RuntimeError(f"PoW solve bad result: {result}")
        return answer

    # ---------------------------------------------------------------- 请求头
    def _base_headers(self, token: str) -> dict[str, str]:
        return {
            "x-client-bundle-id": "com.deepseek.chat",
            "x-client-platform": "web",
            "x-client-version": "2.3.0",
            "x-client-locale": "en_US",
            "x-client-timezone-offset": "28800",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        }

    def _conversation_headers(self, context: DeepSeekContext, pow_header: str) -> dict[str, str]:
        headers = self._base_headers(context.token)
        headers["x-ds-pow-response"] = pow_header
        if context.hif_leim:
            headers["x-hif-leim"] = context.hif_leim
        if context.hif_dliq:
            headers["x-hif-dliq"] = context.hif_dliq
        return headers

    # ---------------------------------------------------------------- 上游日志
    def _upstream_logging_enabled(self) -> bool:
        try:
            return get_setting(self.db, "log_upstream_body", "false").lower() == "true"
        except Exception:
            return False

    def _write_upstream_log(self, kind: str, method: str, url: str, req_body: str,
                            status: int | None, resp_body: str = "", extra: str = "") -> None:
        """log_upstream_body=true 时, 把上游请求/响应写入 logs/upstream_<date>.log"""
        if not self._upstream_logging_enabled():
            return
        try:
            log_dir = Path(__file__).resolve().parent.parent.parent / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            stamp = time.strftime("%Y-%m-%d %H:%M:%S")
            date = time.strftime("%Y%m%d")
            fname = log_dir / f"upstream_{date}.log"
            line = (
                f"[{stamp}] {kind} {method} {url} status={status} {extra}\n"
                f"  req: {req_body[:3000]}\n"
                f"  resp: {resp_body[:3000]}\n"
            )
            with open(fname, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass

    # ---------------------------------------------------------------- 文件上传
    async def upload_file(self, content: bytes, filename: str, token_override: str | None = None,
                          credential_id: int | None = None) -> dict[str, Any]:
        """上传文件到 DeepSeek, 返回 {id, status, file_name, file_size}"""
        context = await self.init_context(token_override=token_override, credential_id=credential_id)
        # upload_file 的 PoW target_path 不同
        try:
            pow_header = await self._get_pow_header(context.token, "/api/v0/file/upload_file")
        except Exception as exc:
            if context.credential_id:
                cred = self.db.get(Credential, context.credential_id)
                if cred:
                    self._mark_credential_failed(cred, exc)
            raise
        headers = self._conversation_headers(context, pow_header)
        headers["x-file-size"] = str(len(content))
        headers["x-model-type"] = "default"
        headers["x-thinking-enabled"] = "0"
        # 移除 Content-Type(让 httpx 用 multipart)
        headers.pop("Content-Type", None)
        # 移除 x-ds-pow-response? 保留(实测前端带)
        files = {"file": (filename, content, "application/octet-stream")}
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(
                f"{self.settings.deepseek_base_url}/api/v0/file/upload_file",
                headers=headers,
                files=files,
            )
            self._write_upstream_log(
                "REQ", "POST", "/api/v0/file/upload_file",
                f"filename={filename} size={len(content)}", r.status_code,
                r.text[:500],
            )
            if r.status_code in (401, 403, 429) and context.credential_id:
                cred = self.db.get(Credential, context.credential_id)
                if cred:
                    self._mark_credential_failed(cred, RuntimeError(f"upload HTTP {r.status_code}"))
            r.raise_for_status()
            j = r.json()
            if j.get("code") != 0:
                raise RuntimeError(f"upload_file failed: code={j.get('code')} msg={j.get('msg')}")
            return j["data"]["biz_data"]

    async def wait_file_ready(self, file_id: str, token_override: str | None = None,
                              credential_id: int | None = None, timeout: float = 30.0) -> dict[str, Any]:
        """轮询 fetch_files 直到 SUCCESS"""
        context = await self.init_context(token_override=token_override, credential_id=credential_id)
        headers = self._base_headers(context.token)
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(
                    f"{self.settings.deepseek_base_url}/api/v0/file/fetch_files",
                    params={"file_ids": file_id},
                    headers=headers,
                )
                r.raise_for_status()
                j = r.json()
                files = j.get("data", {}).get("biz_data", {}).get("files", [])
                if files:
                    last = files[0]
                    if last.get("status") == "SUCCESS":
                        return last
            await asyncio.sleep(1.5)
        raise TimeoutError(f"file {file_id} not ready: {last}")

    # ---------------------------------------------------------------- 会话
    async def _ensure_chat_session(self, context: DeepSeekContext, chat_session_id: str = "") -> str:
        """复用或新建 chat_session; 返回 (session_id, is_new)"""
        if chat_session_id:
            return chat_session_id
        headers = self._base_headers(context.token)
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{self.settings.deepseek_base_url}/api/v0/chat_session/create",
                headers=headers,
                json={},
            )
            r.raise_for_status()
            j = r.json()
            if j.get("code") != 0:
                raise RuntimeError(f"chat_session/create failed: code={j.get('code')} msg={j.get('msg')}")
            return j["data"]["biz_data"]["chat_session"]["id"]

    def _last_response_message_id(self, binding_conversation_id: int | None) -> str:
        """从 conversation 表拿上一轮 response_message_id(存于 last_qid)"""
        if not binding_conversation_id:
            return ""
        conv = self.db.get(BaiduConversation, binding_conversation_id)
        return conv.last_qid if conv else ""

    # ---------------------------------------------------------------- 主对话
    async def stream_conversation(
        self,
        query: str,
        public_model: str,
        direct_answer: Optional[bool] = None,
        baidu_session_id: str = "",
        rank: int | None = None,
        credential_id: int | None = None,
        cookie_override: str | None = None,
        ref_file_ids: list[str] | None = None,
    ) -> AsyncIterator[ParsedEvent]:
        """调用 DeepSeek chat/completion SSE, 产出 ParsedEvent"""
        model = self.get_model(public_model)
        model_type = model.baidu_model or "default"
        context = await self.init_context(token_override=cookie_override, credential_id=credential_id)
        try:
            pow_header = await self._get_pow_header(context.token)
        except Exception as exc:
            # PoW challenge 阶段失败(如 40003 invalid token)说明凭证失效
            if context.credential_id:
                cred = self.db.get(Credential, context.credential_id)
                if cred:
                    self._mark_credential_failed(cred, exc)
            raise
        chat_session_id = await self._ensure_chat_session(context, baidu_session_id)

        # 上一轮 response_message_id(经 binding 持久化在 conversation.last_qid)
        parent_message_id = None
        if baidu_session_id:
            conv = self.db.scalar(
                select(BaiduConversation).where(BaiduConversation.baidu_session_id == baidu_session_id)
            )
            if conv and conv.last_qid:
                try:
                    parent_message_id = int(conv.last_qid)
                except ValueError:
                    parent_message_id = None

        payload = {
            "chat_session_id": chat_session_id,
            "parent_message_id": parent_message_id,
            "model_type": model_type,
            "prompt": query,
            "ref_file_ids": ref_file_ids or [],
            "thinking_enabled": model.think_mode == "1",
            "search_enabled": model.deep_search == "1",
            "action": None,
            "preempt": False,
        }
        headers = self._conversation_headers(context, pow_header)

        if get_setting(self.db, "log_upstream_model", "true").lower() == "true":
            system_log(
                self.db, "INFO", "upstream_model",
                f"deepseek conversation model summary public_model={public_model} "
                f"model_type={model_type} session={chat_session_id[:8]} "
                f"parent={parent_message_id} credential={context.credential_name}",
            )

        req_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        if self._upstream_logging_enabled():
            self._write_upstream_log(
                "REQ", "POST", f"{self.settings.deepseek_base_url}/api/v0/chat/completion",
                req_json, None, extra=f"public_model={public_model}",
            )

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                f"{self.settings.deepseek_base_url}/api/v0/chat/completion",
                headers=headers,
                content=req_json.encode("utf-8"),
            ) as resp:
                if resp.status_code != 200:
                    body = (await resp.aread()).decode(errors="replace")[:500]
                    self._write_upstream_log(
                        "RESP", "POST", resp.url.path, req_json, resp.status_code, body,
                    )
                    # 401/403/4xx 说明凭证失效: 标记失败供轮询跳过
                    if resp.status_code in (401, 403, 429) and context.credential_id:
                        cred = self.db.get(Credential, context.credential_id)
                        if cred:
                            self._mark_credential_failed(cred, RuntimeError(f"DeepSeek HTTP {resp.status_code}: {body[:100]}"))
                    raise RuntimeError(f"DeepSeek completion failed: {resp.status_code} {body}")
                first_lines = ""
                buffer = ""
                response_message_id = ""
                has_emitted_text = False
                fragments: list[dict] = []  # 跟踪 fragment 类型 (THINK / RESPONSE)
                fragment_seen: list[str] = []  # 已下发的 snapshot/append 内容，防前缀丢失或重复
                async for chunk in resp.aiter_bytes():
                    buffer += chunk.decode("utf-8", errors="replace")
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip("\r")
                        if not line:
                            continue
                        if line.startswith("data:"):
                            line = line[5:].strip()
                        if not line:
                            continue
                        if len(first_lines) < 2000:
                            first_lines += line + "\n"
                        event = self.parse_event_line(line)
                        if event is None:
                            continue
                        # quick answer 全文跟在增量流后面会重复, 已输出文本时丢弃
                        if event.raw and event.raw.get("_quick_answer") and has_emitted_text:
                            continue

                        # ---- fragment 跟踪: 决定增量归属 (THINK vs RESPONSE) ----
                        raw = event.raw or {}
                        p = raw.get("p", "")
                        o = raw.get("o", "")
                        v = raw.get("v")
                        if event.component == "snapshot" and isinstance(v, dict):
                            resp_snap = v.get("response") or {}
                            snapshot_fragments = resp_snap.get("fragments")
                            if isinstance(snapshot_fragments, list) and snapshot_fragments:
                                fragments = list(snapshot_fragments)
                                text_delta = ""
                                reasoning_delta = ""
                                next_seen: list[str] = []
                                for index, fragment in enumerate(fragments):
                                    content = fragment.get("content") if isinstance(fragment, dict) else ""
                                    content = content if isinstance(content, str) else ""
                                    previous = fragment_seen[index] if index < len(fragment_seen) else ""
                                    delta = content[len(previous):] if content.startswith(previous) else content
                                    next_seen.append(content)
                                    if not delta:
                                        continue
                                    if isinstance(fragment, dict) and fragment.get("type") == "THINK":
                                        reasoning_delta += delta
                                    else:
                                        text_delta += delta
                                fragment_seen = next_seen
                                if text_delta:
                                    event.text = (event.text or "") + text_delta
                                    event.component = "content"
                                if reasoning_delta:
                                    event.reasoning = (event.reasoning or "") + reasoning_delta
                                    if not event.text:
                                        event.component = "thinking"
                        elif p == "response/fragments" and o == "APPEND" and isinstance(v, list):
                            text_delta = ""
                            reasoning_delta = ""
                            for fragment in v:
                                fragments.append(fragment)
                                content = fragment.get("content") if isinstance(fragment, dict) else ""
                                content = content if isinstance(content, str) else ""
                                fragment_seen.append(content)
                                if isinstance(fragment, dict) and fragment.get("type") == "THINK":
                                    reasoning_delta += content
                                else:
                                    text_delta += content
                            if text_delta:
                                event.text = (event.text or "") + text_delta
                                event.component = "content"
                            if reasoning_delta:
                                event.reasoning = (event.reasoning or "") + reasoning_delta
                                if not event.text:
                                    event.component = "thinking"
                        elif p == "response/fragments/-1/content" and o == "APPEND" and isinstance(v, str):
                            if fragment_seen:
                                fragment_seen[-1] += v

                        # ---- THINK 增量 → reasoning, RESPONSE 增量 → text ----
                        if event.text:
                            current_type = fragments[-1].get("type") if fragments else None
                            if current_type == "THINK":
                                # 思考过程: 不进正文, 收进 reasoning (由 output_reasoning 开关决定是否下发)
                                event.reasoning = (event.reasoning or "") + event.text
                                event.text = ""
                                event.component = "thinking"
                            else:
                                has_emitted_text = True

                        if event.raw:
                            event.raw["_ds_context"] = {
                                "credential_id": context.credential_id,
                                "credential_name": context.credential_name,
                                "chat_session_id": chat_session_id,
                            }
                        if not event.session_id:
                            event.session_id = chat_session_id
                        if response_message_id and not event.qid:
                            event.qid = response_message_id
                        yield event
                        # 记录 response_message_id(来自 event: ready)
                        if event.raw and event.raw.get("_ready"):
                            response_message_id = str(event.raw.get("_ready"))
                # 尾部残留
                if buffer.strip():
                    line = buffer.strip()
                    if line.startswith("data:"):
                        line = line[5:].strip()
                    event = self.parse_event_line(line)
                    if event is not None:
                        if not event.session_id:
                            event.session_id = chat_session_id
                        yield event
                self._write_upstream_log(
                    "RESP", "POST", "/api/v0/chat/completion", req_json, 200,
                    first_lines, extra=f"chars={len(first_lines)}",
                )

    # ---------------------------------------------------------------- SSE 解析
    def parse_event_line(self, line: str) -> ParsedEvent | None:
        """解析 DeepSeek SSE 行(JSON 或带 event: 前缀)"""
        try:
            data = orjson.loads(line)
        except Exception:
            return None

        # event: ready / close 由调用方剥离? 这里 data 是 JSON 对象
        # 结构1: {"request_message_id":1,"response_message_id":2,"model_type":"default"}  (event: ready)
        if "request_message_id" in data and "response_message_id" in data:
            event = ParsedEvent(component="ready", raw=data, finished=False)
            event.raw["_ready"] = data.get("response_message_id")
            return event

        # 结构2: {"updated_at": ...}  (event: update_session)
        if "updated_at" in data and "v" not in data:
            return ParsedEvent(component="update_session", raw=data, finished=False)

        # 结构4(需在纯 v 字符串之前): {"p":..., "o":..., "v":...}  状态/增量操作
        if "p" in data and "o" in data:
            op = data.get("o")
            path = data.get("p")
            value = data.get("v")
            if op == "APPEND" and path == "response/fragments/-1/content" and isinstance(value, str):
                return ParsedEvent(component="content", text=value, raw=data, finished=False)
            if op == "SET" and path == "response/status" and value == "FINISHED":
                return ParsedEvent(component="finish", raw=data, finished=True)
            if op == "BATCH" and isinstance(value, list):
                # 检查是否包含 FINISHED 状态
                for item in value:
                    if isinstance(item, dict) and item.get("p") == "quasi_status" and item.get("v") == "FINISHED":
                        return ParsedEvent(component="finish", raw=data, finished=True)
                return ParsedEvent(component="batch", raw=data, finished=False)
            return ParsedEvent(component="other", raw=data, finished=False)

        # 结构4b(无 o 字段的纯状态): {"p":"response/fragments/-1/status","v":"FINISHED"}
        # 和 {"p":"response/conversation_mode","v":"SEARCH"} 等 — 不应作为正文
        if "p" in data and isinstance(data.get("p"), str) and "v" in data:
            path = data.get("p")
            value = data.get("v")
            if path.endswith("/status"):
                if value == "FINISHED":
                    return ParsedEvent(component="finish", raw=data, finished=True)
                return ParsedEvent(component="status", raw=data, finished=False)
            if path in ("response/conversation_mode", "response/has_pending_fragment", "response/auto_continue"):
                return ParsedEvent(component="meta", raw=data, finished=False)
            if path.endswith("/results") and isinstance(value, list):
                # 联网搜索结果(引用来源), 不进入正文
                return ParsedEvent(component="search_results", raw=data, finished=False)

        # 结构3: {"v": "文本增量"} (无 p/o 时才是纯文本)
        if "v" in data and isinstance(data.get("v"), str):
            return ParsedEvent(component="content", text=data["v"], raw=data, finished=False)

        # 结构5: {"v": {...response snapshot...}}  消息快照
        if "v" in data and isinstance(data.get("v"), dict):
            snap = data["v"]
            resp = snap.get("response") if isinstance(snap, dict) else None
            if isinstance(resp, dict) and resp.get("status") == "FINISHED":
                return ParsedEvent(component="finish", raw=data, finished=True)
            return ParsedEvent(component="snapshot", raw=data, finished=False)

        # 结构6: {"click_behavior": ...}  (event: close)
        if "click_behavior" in data or "auto_resume" in data:
            return ParsedEvent(component="close", raw=data, finished=True)

        # 结构7: {"content": "完整回答"}  (quick answer 模式, 无逐字增量)
        if "content" in data and isinstance(data.get("content"), str) and "v" not in data:
            event = ParsedEvent(component="content", text=data["content"], raw=data, finished=True)
            event.raw["_quick_answer"] = True
            return event

        return ParsedEvent(component="unknown", raw=data, finished=False)

    # ---------------------------------------------------------------- 未使用接口
    async def canvas_search(self, *args, **kwargs):
        raise NotImplementedError("DeepSeek adapter does not support canvas")

    async def download_workspace_file(self, *args, **kwargs):
        raise NotImplementedError("DeepSeek adapter does not support workspace download")
