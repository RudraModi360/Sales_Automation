import importlib
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)


@dataclass
class LLMGenerationResult:
    structured_output: dict[str, Any]
    usage: dict[str, Any]
    raw_response: Any = None


@dataclass
class LLMSessionState:
    session_id: str
    message_history: list[dict[str, str]] = field(default_factory=list)
    provider_state: dict[str, Any] = field(default_factory=dict)
    tool_trace: list[dict[str, Any]] = field(default_factory=list)

    def append_turn(self, user_content: str, assistant_content: str, max_messages: int = 20) -> None:
        self.message_history.append({"role": "user", "content": user_content})
        self.message_history.append({"role": "assistant", "content": assistant_content})
        if len(self.message_history) > max_messages:
            self.message_history = self.message_history[-max_messages:]

    def clear(self) -> None:
        self.message_history.clear()
        self.provider_state.clear()
        self.tool_trace.clear()


class ToolRegistry:
    """Simple tool registry for provider-agnostic tool calling workflows."""

    def __init__(self) -> None:
        self._tools: dict[str, dict[str, Any]] = {}

    def register(
        self,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        handler: Callable[[dict[str, Any]], Any],
    ) -> None:
        if not name or not callable(handler):
            raise ValueError("Tool requires a name and callable handler")

        self._tools[name] = {
            "name": name,
            "description": description,
            "input_schema": input_schema,
            "handler": handler,
        }

    def list_tool_specs(self) -> list[dict[str, Any]]:
        return [
            {
                "name": spec["name"],
                "description": spec["description"],
                "input_schema": spec["input_schema"],
            }
            for spec in self._tools.values()
        ]

    def invoke(self, name: str, arguments: dict[str, Any]) -> Any:
        if name not in self._tools:
            raise ValueError(f"Tool '{name}' is not registered")
        return self._tools[name]["handler"](arguments)


class SessionStore:
    """In-memory session store for reusable LLM chain context."""

    def __init__(self) -> None:
        self._sessions: dict[str, LLMSessionState] = {}

    def get_or_create(self, session_id: Optional[str] = None) -> LLMSessionState:
        if not session_id:
            return LLMSessionState(session_id=f"ephemeral-{uuid.uuid4().hex}")

        if session_id not in self._sessions:
            self._sessions[session_id] = LLMSessionState(session_id=session_id)

        return self._sessions[session_id]

    def reset(self, session_id: str) -> None:
        if session_id in self._sessions:
            self._sessions[session_id].clear()


SESSION_STORE = SessionStore()


def get_session_state(session_id: Optional[str] = None) -> LLMSessionState:
    return SESSION_STORE.get_or_create(session_id=session_id)


def reset_session_state(session_id: str) -> None:
    SESSION_STORE.reset(session_id=session_id)


def _history_as_text(session_state: Optional[LLMSessionState], max_turns: int = 6) -> str:
    if session_state is None or not session_state.message_history:
        return ""

    recent_history = session_state.message_history[-(max_turns * 2):]
    lines: list[str] = []
    for item in recent_history:
        role = item.get("role", "user").strip().lower()
        role_label = "User" if role == "user" else "Assistant"
        lines.append(f"{role_label}: {item.get('content', '')}")

    return "\n".join(lines)


def _parse_json_payload(raw_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_text)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        pass

    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            payload = json.loads(raw_text[start : end + 1])
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse JSON payload: {exc}") from exc

    raise RuntimeError("Model output does not contain a valid JSON object")


class BaseLLMProvider:
    provider_name = "base"

    def generate_structured(
        self,
        prompt: str,
        output_model: type[BaseModel],
        format_instructions: str,
        session_state: Optional[LLMSessionState] = None,
        use_cache: bool = False,
        static_context: Optional[str] = None,
        max_tokens: int = 2048,
    ) -> LLMGenerationResult:
        raise NotImplementedError


class AnthropicProvider(BaseLLMProvider):
    provider_name = "anthropic"

    def __init__(self, client: Any = None, model: Optional[str] = None) -> None:
        self.client = client or self._build_client()
        self.model = model or os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

    @staticmethod
    def _build_client() -> Any:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return None

        try:
            anthropic_module = importlib.import_module("anthropic")
        except ModuleNotFoundError:
            return None

        return anthropic_module.Anthropic(api_key=api_key)

    def generate_structured(
        self,
        prompt: str,
        output_model: type[BaseModel],
        format_instructions: str,
        session_state: Optional[LLMSessionState] = None,
        use_cache: bool = False,
        static_context: Optional[str] = None,
        max_tokens: int = 2048,
    ) -> LLMGenerationResult:
        if self.client is None:
            raise RuntimeError(
                "Anthropic client is not configured. Set ANTHROPIC_API_KEY and install anthropic."
            )

        history_text = "" if use_cache else _history_as_text(session_state)
        runtime_prompt = prompt
        if history_text:
            runtime_prompt = (
                "Conversation state from previous turns:\n"
                f"{history_text}\n\n"
                "Current task:\n"
                f"{prompt}"
            )

        if use_cache and static_context:
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": static_context,
                            "cache_control": {"type": "ephemeral"},
                        },
                        {"type": "text", "text": runtime_prompt},
                        {
                            "type": "text",
                            "text": f"Format Instructions:\n{format_instructions}",
                            "cache_control": {"type": "ephemeral"},
                        },
                    ],
                }
            ]
        else:
            messages = [
                {
                    "role": "user",
                    "content": f"{runtime_prompt}\n\n{format_instructions}",
                }
            ]

        response = self.client.messages.parse(
            model=self.model,
            max_tokens=max_tokens,
            messages=messages,
            output_format=output_model,
        )

        parsed_output = getattr(response, "parsed_output", None)
        if not parsed_output:
            raise RuntimeError("No structured output returned by Anthropic parse API")

        try:
            structured_output = output_model.model_validate(parsed_output).model_dump()
        except ValidationError as exc:
            raise RuntimeError(f"Anthropic output validation failed: {exc}") from exc

        usage = {
            "input_tokens": getattr(response.usage, "input_tokens", 0),
            "cache_creation_input_tokens": getattr(response.usage, "cache_creation_input_tokens", 0),
            "cache_read_input_tokens": getattr(response.usage, "cache_read_input_tokens", 0),
            "output_tokens": getattr(response.usage, "output_tokens", 0),
        }

        return LLMGenerationResult(
            structured_output=structured_output,
            usage=usage,
            raw_response=response,
        )


class OllamaProvider(BaseLLMProvider):
    provider_name = "ollama"

    def __init__(self, model: Optional[str] = None, host: Optional[str] = None) -> None:
        self.model = model or os.getenv("OLLAMA_MODEL", "qwen3:0.6b")
        self.host = host or os.getenv("OLLAMA_HOST")
        self.client = self._build_client()

    def _build_client(self) -> Any:
        try:
            ollama_module = importlib.import_module("ollama")
        except ModuleNotFoundError:
            return None

        if hasattr(ollama_module, "Client"):
            if self.host:
                return ollama_module.Client(host=self.host)
            return ollama_module.Client()

        return ollama_module

    def generate_structured(
        self,
        prompt: str,
        output_model: type[BaseModel],
        format_instructions: str,
        session_state: Optional[LLMSessionState] = None,
        use_cache: bool = False,
        static_context: Optional[str] = None,
        max_tokens: int = 2048,
    ) -> LLMGenerationResult:
        if self.client is None:
            raise RuntimeError(
                "Ollama client is not configured. Install `ollama` package and ensure Ollama is running."
            )

        strict_prefix_cache = (
            os.getenv("EMAIL_REQUIRE_PREFIX_CONTEXT_CACHE", "false").strip().lower() == "true"
        )

        context_tokens = None
        if use_cache and session_state is not None:
            context_tokens = session_state.provider_state.get("ollama_context")

        history_text = _history_as_text(session_state)
        prompt_parts: list[str] = []

        # Only include static context on first cached request; after that, context tokens carry it.
        if static_context and (not use_cache or context_tokens is None):
            prompt_parts.append(f"STATIC CONTEXT\n{static_context}")

        if history_text and not use_cache:
            prompt_parts.append(f"Conversation state from previous turns:\n{history_text}")

        prompt_parts.append(prompt)
        prompt_parts.append(format_instructions)

        prompt_text = "\n\n".join(prompt_parts)

        request_payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt_text,
            "format": "json",
            "stream": False,
            "options": {"num_predict": max_tokens},
        }

        if context_tokens is not None:
            request_payload["context"] = context_tokens

        response = self.client.generate(**request_payload)

        raw_response_text = str(response.get("response", "") or "").strip()
        parsed_payload = _parse_json_payload(raw_response_text)

        try:
            structured_output = output_model.model_validate(parsed_payload).model_dump()
        except ValidationError as exc:
            raise RuntimeError(f"Ollama output validation failed: {exc}") from exc

        returned_context = response.get("context")
        if use_cache and session_state is not None:
            if returned_context is not None:
                session_state.provider_state["ollama_context"] = returned_context
                session_state.provider_state["prefix_cache_available"] = True
                session_state.provider_state.pop("prefix_cache_warned", None)
            else:
                session_state.provider_state.pop("ollama_context", None)
                session_state.provider_state["prefix_cache_available"] = False

                warning_message = (
                    "Ollama did not return context tokens for this request. "
                    "Falling back to previous-email prompt continuity."
                )

                if not session_state.provider_state.get("prefix_cache_warned"):
                    logger.warning(warning_message)
                    session_state.provider_state["prefix_cache_warned"] = True

                if strict_prefix_cache:
                    raise RuntimeError(
                        warning_message
                        + " Strict mode is enabled via EMAIL_REQUIRE_PREFIX_CONTEXT_CACHE=true."
                    )

        usage = {
            "input_tokens": int(response.get("prompt_eval_count", 0) or 0),
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "output_tokens": int(response.get("eval_count", 0) or 0),
            "prompt_eval_duration_ns": int(response.get("prompt_eval_duration", 0) or 0),
            "eval_duration_ns": int(response.get("eval_duration", 0) or 0),
            "prefix_cache_reused": bool(context_tokens is not None and returned_context is not None),
            "context_tokens_in": (
                len(context_tokens)
                if isinstance(context_tokens, list)
                else int(context_tokens is not None)
            ),
            "context_tokens_out": (
                len(returned_context)
                if isinstance(returned_context, list)
                else int(returned_context is not None)
            ),
            "prompt_char_count": len(prompt_text),
        }

        return LLMGenerationResult(
            structured_output=structured_output,
            usage=usage,
            raw_response=response,
        )


def build_provider(
    provider_name: Optional[str] = None,
    anthropic_client: Any = None,
) -> BaseLLMProvider:
    normalized = (provider_name or os.getenv("LLM_PROVIDER") or "anthropic").strip().lower()

    if normalized == "anthropic":
        return AnthropicProvider(client=anthropic_client)

    if normalized == "ollama":
        return OllamaProvider()

    raise ValueError(
        f"Unsupported provider '{normalized}'. Supported providers: anthropic, ollama"
    )
