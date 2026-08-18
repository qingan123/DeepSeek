from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class ChatMessage(BaseModel):
    role: str
    content: Any = ""
    name: Optional[str] = None
    tool_call_id: Optional[str] = None
    tool_calls: Optional[list[dict[str, Any]]] = None


class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    stream: bool = False
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    tools: Optional[list[dict[str, Any]]] = None
    tool_choice: Optional[Any] = None
    parallel_tool_calls: Optional[bool] = None
    user: Optional[str] = None
    conversation_id: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class ResponseInputMessage(BaseModel):
    # Responses API input contains heterogeneous items (message,
    # function_call, function_call_output, computer_call_output, ...).
    # Preserve unknown/new fields so the API adapter can translate them.
    model_config = ConfigDict(extra="allow")

    type: Optional[str] = None
    role: Optional[str] = None
    content: Any = ""
    call_id: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = None
    arguments: Any = None
    output: Any = None


class ResponsesRequest(BaseModel):
    model: str
    input: str | list[ResponseInputMessage] | list[dict[str, Any]]
    stream: bool = False
    instructions: Optional[str] = None
    temperature: Optional[float] = None
    max_output_tokens: Optional[int] = None
    tools: Optional[list[dict[str, Any]]] = None
    tool_choice: Optional[Any] = None
    parallel_tool_calls: Optional[bool] = None
    user: Optional[str] = None
    conversation_id: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class ImageGenerationRequest(BaseModel):
    model: str = "miaotu"
    prompt: str
    n: int = Field(default=1, ge=1, le=4)
    size: str = "1024x1024"
    response_format: Literal["url", "b64_json"] = "url"
