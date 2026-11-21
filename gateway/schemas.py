from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class Function(Strict):
    name: str = Field(min_length=1, max_length=64)
    description: str | None = Field(default=None, max_length=4000)
    parameters: dict[str, Any]


class Tool(Strict):
    type: Literal["function"]
    function: Function


class CalledFunction(Strict):
    name: str = Field(min_length=1, max_length=64)
    arguments: str = Field(max_length=100000)


class ToolCall(Strict):
    id: str = Field(min_length=1, max_length=200)
    type: Literal["function"]
    function: CalledFunction


class Message(Strict):
    role: Literal["system", "user", "assistant", "tool"]
    content: str | None = Field(default=None, max_length=100000)
    tool_calls: list[ToolCall] | None = Field(default=None, min_length=1, max_length=32)
    tool_call_id: str | None = None

    @model_validator(mode="after")
    def coherent(self):
        if self.role != "assistant" and self.content is None:
            raise ValueError("content required")
        if self.role == "tool" and not self.tool_call_id:
            raise ValueError("tool message requires tool_call_id")
        if self.tool_calls and self.role != "assistant":
            raise ValueError("only assistant can send tool_calls")
        if self.tool_call_id and self.role != "tool":
            raise ValueError("only tool messages can send tool_call_id")
        if self.role == "assistant" and self.content is None and not self.tool_calls:
            raise ValueError("assistant requires content or tool_calls")
        return self


class ResponseFormat(Strict):
    type: Literal["json_object", "text"]


class ChatRequest(Strict):
    model: str = Field(min_length=1, max_length=100)
    messages: list[Message] = Field(min_length=1, max_length=100)
    stream: bool = False
    temperature: float = Field(default=1, ge=0, le=2)
    max_tokens: int = Field(default=1024, ge=1, le=8192)
    response_format: ResponseFormat | None = None
    tools: list[Tool] | None = Field(default=None, min_length=1, max_length=32)
    tool_choice: Literal["auto", "none", "required"] | None = None

    @model_validator(mode="after")
    def supported_combination(self):
        tool_context = self.tools or any(m.tool_calls or m.role == "tool" for m in self.messages)
        if self.stream and (tool_context or self.response_format and self.response_format.type == "json_object"):
            raise ValueError("stream supports text only")
        if self.tool_choice is not None and not self.tools:
            raise ValueError("tool_choice requires tools")
        if self.tools and self.response_format and self.response_format.type == "json_object":
            raise ValueError("tools plus JSON mode is unsupported")
        return self

    def capabilities(self):
        required = {"text"}
        if self.stream:
            required.add("stream")
        if self.tools or any(m.tool_calls or m.role == "tool" for m in self.messages):
            required.add("tools")
        if self.response_format and self.response_format.type == "json_object":
            required.add("json")
        return required
