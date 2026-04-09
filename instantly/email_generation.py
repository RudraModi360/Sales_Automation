import os
import importlib
import json
import logging
import time
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from groq import Groq
from pydantic import BaseModel, ValidationError

from .prompts import (
    email_prompt as build_email_prompt,
    followup_1_prompt,
    followup_2_prompt,
    followup_3_prompt,
    followup_4_prompt,
)

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("email_chain_generation.log"),
    ],
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_OLLAMA_MODEL = "gpt-oss:20b-cloud"
DEFAULT_OLLAMA_TEMPERATURE = 0.0
DEFAULT_MAX_TOKENS = 2048
DEFAULT_OLLAMA_KEEP_ALIVE = "30m"
DEFAULT_EMAIL_GENERATION_MAX_RETRIES = 3
DEFAULT_EMAIL_RETRY_DELAY_SECONDS = 1.0
DEFAULT_EMAIL_REQUIRE_PREFIX_CONTEXT_CACHE = False
DEFAULT_EMAIL_PREFIX_CACHE_USE_STREAM = True

STRUCTURED_OUTPUT_FORMAT = """Return ONLY structured JSON with exactly these keys:
- introduction
- value_proposition
- call_to_action

Constraints:
- introduction: 1-2 sentences.
- value_proposition: 2-3 sentences based on the provided context.
- call_to_action: 1-2 sentences with a clear next step.
- No markdown, no extra keys."""

EMAIL_SEQUENCE = [
    (build_email_prompt, "main_email", "Initial Outreach Email"),
    (followup_1_prompt, "followup_1", "Follow-up 1: Different Angle"),
    (followup_2_prompt, "followup_2", "Follow-up 2: New Value"),
    (followup_3_prompt, "followup_3", "Follow-up 3: Urgency"),
    (followup_4_prompt, "followup_4", "Follow-up 4: Final Attempt"),
]

CACHE_MODE_EMAIL_INSTRUCTIONS = {
    "main_email": (
        "Generate the initial outreach email for the same prospect context already in memory. "
        "Keep it concise and personalized with a professional tone."
    ),
    "followup_1": (
        "Generate follow-up 1 for the same thread. Softly reference the prior email, "
        "use a different angle, and avoid repeating wording."
    ),
    "followup_2": (
        "Generate follow-up 2 for the same thread. Assume they are busy, add new value "
        "or proof, and include a low-friction CTA."
    ),
    "followup_3": (
        "Generate follow-up 3 for the same thread. Add respectful, relevant urgency "
        "without sounding pushy."
    ),
    "followup_4": (
        "Generate the final follow-up for the same thread. Be respectful, include one "
        "last useful angle, and end with a graceful exit."
    ),
}


def _build_ollama_client():
    """Build Ollama client for local or cloud usage based on env vars."""
    try:
        ollama_module = importlib.import_module("ollama")
    except ModuleNotFoundError:
        return None

    client_class = getattr(ollama_module, "Client", None)
    if client_class is None:
        return None

    host = (os.getenv("OLLAMA_HOST") or "").strip()
    api_key = (os.getenv("OLLAMA_API_KEY") or "").strip()

    if api_key:
        return client_class(
            host=host or "https://ollama.com",
            headers={"Authorization": f"Bearer {api_key}"},
        )

    if host:
        return client_class(host=host)

    return client_class()


client = _build_ollama_client()

client_groq = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)


class EmailStructuredOutput(BaseModel):
    introduction: str
    value_proposition: str
    call_to_action: str


def _record_to_dict(record: Any) -> dict[str, Any]:
    """Convert row-like objects (Series/dict/custom) into plain dict for prompt serialization."""
    if hasattr(record, "to_dict"):
        return record.to_dict()
    if isinstance(record, dict):
        return dict(record)
    raise TypeError("Expected df to be dict-like or expose to_dict().")


def _parse_structured_email_json(raw_content: str) -> dict[str, Any]:
    """Parse model content into EmailStructuredOutput, tolerating extra text wrappers."""
    if not raw_content or not str(raw_content).strip():
        raise RuntimeError("Model did not return content for structured output parsing.")

    text = str(raw_content).strip()

    try:
        return EmailStructuredOutput.model_validate_json(text).model_dump()
    except ValidationError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise RuntimeError("Model response was not valid JSON for EmailStructuredOutput.")
        return EmailStructuredOutput.model_validate_json(text[start : end + 1]).model_dump()


def person_data_explorer(client, df):
    """Extract relevant person and company context using Groq."""
    person_fields = {
        "first_name": df["first_name"],
        "last_name": df["last_name"],
        "job_title": df["job_title"],
        "seniority": df["seniority"],
        "company": df["company_name"],
        "company_description": df["company_description"],
        "company_website": df["company_website"],
        "person_linkedin": df["person_linkedin"],
        "person_twitter": df["person_twitter"],
        "company_linkedin": df["company_linkedin"],
        "company_industry": df["company_industry"],
    }

    fields_str = "\n".join(f'"{k}": {v},' for k, v in person_fields.items())

    completion = client.chat.completions.create(
        model="groq/compound-mini",
        messages=[
            {
                "role": "user",
                "content": f"""Analyze this person's profile and extract the most relevant information:

{fields_str}

Visit the provided links if available to gather additional context about the user and company.

Provide a brief summary (max 500 words) about the person and company. Be concise with token usage.""",
            }
        ],
    )
    return completion.choices[0].message.content


def _build_email_prompt_with_context(
    prompt_fn,
    df,
    person_context,
    previous_email=None,
    include_previous_email: bool = True,
):
    """Build prompt for email generation with optional previous-email inclusion."""
    current_prompt = prompt_fn(df, person_context)

    if include_previous_email and previous_email:
        current_prompt += f"""

---
CONTEXT FROM PREVIOUS EMAIL:

{previous_email}

Build on this context in your new email. Reference if appropriate, but provide fresh value, avoid repetition, and escalate as needed.
---"""

    return current_prompt


def _extract_email_text(structured_output):
    """Combine structured output into readable email text."""
    return f"{structured_output['introduction']}\n\n{structured_output['value_proposition']}\n\n{structured_output['call_to_action']}"


def _calculate_cache_efficiency(usage_stats):
    """Calculate cache-like metrics from token usage schema."""
    total_input = usage_stats["input_tokens"]
    cache_read = usage_stats["cache_read_input_tokens"]
    new_tokens = total_input - cache_read

    cache_hit_rate = f"{(cache_read / total_input * 100):.1f}%" if total_input > 0 else "0%"

    return {
        "cache_creation_tokens": usage_stats["cache_creation_input_tokens"],
        "cache_read_tokens": cache_read,
        "new_input_tokens": new_tokens,
        "total_input_tokens": total_input,
        "cache_hit_rate": cache_hit_rate,
    }


def _build_ollama_prompt(
    prompt_fn,
    df,
    person_context,
    previous_email=None,
    include_previous_email: bool = True,
):
    """Build a single prompt string for Ollama with explicit schema grounding."""
    current_prompt = _build_email_prompt_with_context(
        prompt_fn,
        df,
        person_context,
        previous_email=previous_email,
        include_previous_email=include_previous_email,
    )

    return _build_structured_output_prompt(current_prompt)


def _build_structured_output_prompt(core_prompt: str) -> str:
    """Attach JSON schema and output constraints to a prompt body."""
    schema_text = json.dumps(EmailStructuredOutput.model_json_schema(), indent=2)

    return (
        f"{core_prompt}\n\n"
        f"JSON schema to follow exactly:\n{schema_text}\n\n"
        f"{STRUCTURED_OUTPUT_FORMAT}\n"
        "Return valid JSON only."
    )


def _build_cache_mode_iteration_prompt(email_type: str, description: str) -> str:
    """Build compact per-iteration prompt used when Ollama context tokens are reusable."""
    sequence_instruction = CACHE_MODE_EMAIL_INSTRUCTIONS.get(
        email_type,
        f"Generate {description} for the same outreach thread context already in memory.",
    )

    return (
        "Continue the exact same recipient and company context from prior turns. "
        f"{sequence_instruction} "
        "Do not repeat prior wording. Produce fresh copy that progresses the sequence."
    )


def _extract_ollama_message_content(response: Any) -> str:
    """Normalize Ollama chat response message content across object/dict formats."""
    if isinstance(response, dict):
        message = response.get("message") or {}
        return str(message.get("content") or "")

    message = getattr(response, "message", None)
    if isinstance(message, dict):
        return str(message.get("content") or "")

    return str(getattr(message, "content", "") or "")


def _extract_ollama_generate_content(response: Any) -> str:
    """Extract text content from Ollama generate response."""
    if isinstance(response, dict):
        return str(response.get("response") or "")
    return str(getattr(response, "response", "") or "")


def _extract_ollama_context_tokens(response: Any) -> list[int] | None:
    """Extract context tokens from Ollama generate response when available."""
    if isinstance(response, dict):
        context_tokens = response.get("context")
    else:
        context_tokens = getattr(response, "context", None)
        if context_tokens is None and hasattr(response, "model_dump"):
            dumped = response.model_dump()
            if isinstance(dumped, dict):
                context_tokens = dumped.get("context")

    if isinstance(context_tokens, list):
        return context_tokens

    if isinstance(context_tokens, tuple):
        return list(context_tokens)

    return None


def _extract_ollama_usage(response: Any) -> dict[str, int]:
    """Map Ollama token usage fields to the shared usage schema."""
    if isinstance(response, dict):
        input_tokens = int(response.get("prompt_eval_count") or 0)
        output_tokens = int(response.get("eval_count") or 0)
    else:
        input_tokens = int(getattr(response, "prompt_eval_count", 0) or 0)
        output_tokens = int(getattr(response, "eval_count", 0) or 0)

    context_tokens = _extract_ollama_context_tokens(response)

    return {
        "input_tokens": input_tokens,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "output_tokens": output_tokens,
        "prefix_context_tokens": len(context_tokens) if context_tokens else 0,
    }


def _ollama_generation_options() -> dict[str, Any]:
    """Generation options for Ollama chat requests."""
    try:
        temperature = float(os.getenv("OLLAMA_TEMPERATURE", str(DEFAULT_OLLAMA_TEMPERATURE)))
    except ValueError:
        temperature = DEFAULT_OLLAMA_TEMPERATURE

    try:
        num_predict = int(os.getenv("OLLAMA_MAX_TOKENS", str(DEFAULT_MAX_TOKENS)))
    except ValueError:
        num_predict = DEFAULT_MAX_TOKENS

    return {
        "temperature": temperature,
        "num_predict": num_predict,
    }


def _read_env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw_value = os.getenv(name)
    try:
        parsed = int(raw_value) if raw_value is not None else default
    except ValueError:
        parsed = default

    if minimum is not None:
        return max(minimum, parsed)

    return parsed


def _read_env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw_value = os.getenv(name)
    try:
        parsed = float(raw_value) if raw_value is not None else default
    except ValueError:
        parsed = default

    if minimum is not None:
        return max(minimum, parsed)

    return parsed


def _read_env_bool(name: str, default: bool, truthy: set[str], falsy: set[str]) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False

    return default


def _get_email_generation_max_retries() -> int:
    return _read_env_int(
        "EMAIL_GENERATION_MAX_RETRIES",
        DEFAULT_EMAIL_GENERATION_MAX_RETRIES,
        minimum=1,
    )


def _get_email_retry_delay_seconds() -> float:
    return _read_env_float(
        "EMAIL_GENERATION_RETRY_DELAY_SECONDS",
        DEFAULT_EMAIL_RETRY_DELAY_SECONDS,
        minimum=0.0,
    )


def _is_prefix_context_cache_enabled() -> bool:
    return _read_env_bool(
        "EMAIL_USE_PREFIX_CONTEXT_CACHE",
        True,
        truthy={"1", "true", "yes", "on"},
        falsy={"0", "false", "no", "off"},
    )


def _is_prefix_context_cache_required() -> bool:
    return _read_env_bool(
        "EMAIL_REQUIRE_PREFIX_CONTEXT_CACHE",
        DEFAULT_EMAIL_REQUIRE_PREFIX_CONTEXT_CACHE,
        truthy={"1", "true", "yes", "on"},
        falsy={"0", "false", "no", "off"},
    )


def _is_prefix_cache_stream_enabled() -> bool:
    return _read_env_bool(
        "EMAIL_PREFIX_CACHE_USE_STREAM",
        DEFAULT_EMAIL_PREFIX_CACHE_USE_STREAM,
        truthy={"1", "true", "yes", "on"},
        falsy={"0", "false", "no", "off"},
    )


def _get_ollama_keep_alive() -> str | None:
    keep_alive = (os.getenv("OLLAMA_KEEP_ALIVE") or DEFAULT_OLLAMA_KEEP_ALIVE).strip()
    return keep_alive or None


def _get_ollama_model_name() -> str:
    email_model = (os.getenv("OLLAMA_EMAIL_MODEL") or "").strip()
    if email_model:
        return email_model
    return (os.getenv("OLLAMA_MODEL") or "").strip() or DEFAULT_OLLAMA_MODEL


def _invoke_with_model_fallback(
    request_fn,
    model_name: str,
    fallback_warning: str,
) -> tuple[Any, str]:
    """Run a model request and retry once with DEFAULT_OLLAMA_MODEL if configured model is unavailable."""
    active_model_name = model_name

    try:
        return request_fn(active_model_name), active_model_name
    except Exception as exc:
        if _is_model_not_found_error(exc) and active_model_name != DEFAULT_OLLAMA_MODEL:
            logger.warning(fallback_warning, active_model_name, DEFAULT_OLLAMA_MODEL)
            active_model_name = DEFAULT_OLLAMA_MODEL
            return request_fn(active_model_name), active_model_name
        raise


def _normalize_ollama_chunk(chunk: Any) -> dict[str, Any]:
    """Normalize streamed Ollama chunk into a plain dict."""
    if isinstance(chunk, dict):
        return chunk

    if hasattr(chunk, "model_dump"):
        dumped = chunk.model_dump()
        if isinstance(dumped, dict):
            return dumped

    return {
        "response": getattr(chunk, "response", ""),
        "context": getattr(chunk, "context", None),
        "prompt_eval_count": getattr(chunk, "prompt_eval_count", None),
        "eval_count": getattr(chunk, "eval_count", None),
    }


def _consume_ollama_generate_stream(stream_response: Any) -> dict[str, Any]:
    """Consume streamed generate response and return final normalized payload."""
    full_response = ""
    last_chunk: dict[str, Any] | None = None
    latest_context: list[int] | None = None
    latest_prompt_eval_count: int | None = None
    latest_eval_count: int | None = None

    for chunk in stream_response:
        normalized_chunk = _normalize_ollama_chunk(chunk)
        token = str(normalized_chunk.get("response") or "")
        if token:
            full_response += token

        context_tokens = normalized_chunk.get("context")
        if isinstance(context_tokens, tuple):
            context_tokens = list(context_tokens)
        if isinstance(context_tokens, list):
            latest_context = context_tokens

        prompt_eval_count = normalized_chunk.get("prompt_eval_count")
        if prompt_eval_count is not None:
            latest_prompt_eval_count = int(prompt_eval_count)

        eval_count = normalized_chunk.get("eval_count")
        if eval_count is not None:
            latest_eval_count = int(eval_count)

        last_chunk = normalized_chunk

    payload = dict(last_chunk or {})
    payload["response"] = full_response

    if latest_context is not None:
        payload["context"] = latest_context
    if latest_prompt_eval_count is not None:
        payload["prompt_eval_count"] = latest_prompt_eval_count
    if latest_eval_count is not None:
        payload["eval_count"] = latest_eval_count

    return payload


def _call_ollama_generate(ollama_client: Any, model_name: str, kwargs: dict[str, Any]) -> Any:
    """Call Ollama generate with compatibility for different client signatures."""
    try:
        return ollama_client.generate(model=model_name, **kwargs)
    except TypeError:
        return ollama_client.generate(model_name, **kwargs)


def _ollama_chat_with_schema(ollama_client: Any, model_name: str, prompt: str, keep_alive: str | None = None) -> Any:
    """Call Ollama chat using schema format with compatibility for client signatures."""
    kwargs = {
        "messages": [{"role": "user", "content": prompt}],
        "format": EmailStructuredOutput.model_json_schema(),
        "options": _ollama_generation_options(),
        "stream": False,
    }

    if keep_alive is not None:
        kwargs["keep_alive"] = keep_alive

    try:
        return ollama_client.chat(model=model_name, **kwargs)
    except TypeError:
        return ollama_client.chat(model_name, **kwargs)


def _ollama_generate_with_schema(
    ollama_client: Any,
    model_name: str,
    prompt: str,
    context_tokens: list[int] | None = None,
    keep_alive: str | None = None,
) -> Any:
    """Call Ollama generate API using schema format and optional context token reuse."""
    use_stream = _is_prefix_cache_stream_enabled()

    kwargs = {
        "prompt": prompt,
        "format": EmailStructuredOutput.model_json_schema(),
        "options": _ollama_generation_options(),
        "stream": use_stream,
    }

    if context_tokens:
        kwargs["context"] = context_tokens

    if keep_alive is not None:
        kwargs["keep_alive"] = keep_alive

    response = _call_ollama_generate(
        ollama_client=ollama_client,
        model_name=model_name,
        kwargs=kwargs,
    )

    if use_stream:
        return _consume_ollama_generate_stream(response)

    return response


def _is_model_not_found_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return "model" in message and "not found" in message and "404" in message


def _generate_email_internal(
    ollama_client,
    df,
    person_context,
    prompt_fn,
    previous_email=None,
    context_tokens: list[int] | None = None,
    use_prefix_context_cache: bool | None = None,
    prompt_override: str | None = None,
    max_attempts: int | None = None,
) -> dict:
    """Unified internal function for email generation using Ollama structured output."""
    if ollama_client is None:
        raise RuntimeError(
            "Ollama client is not configured. Install ollama and set OLLAMA_HOST/OLLAMA_API_KEY as needed."
        )

    model_name = _get_ollama_model_name()

    if max_attempts is None:
        max_attempts = _get_email_generation_max_retries()
    max_attempts = max(1, int(max_attempts))

    if use_prefix_context_cache is None:
        use_prefix_context_cache = _is_prefix_context_cache_enabled()

    active_context_tokens = context_tokens

    if prompt_override is not None:
        base_prompt = _build_structured_output_prompt(prompt_override)
    else:
        # In prefix-cache mode, avoid appending previous-email text into prompt bodies.
        include_previous_email_in_prompt = not use_prefix_context_cache

        base_prompt = _build_ollama_prompt(
            prompt_fn,
            df,
            person_context,
            previous_email=previous_email,
            include_previous_email=include_previous_email_in_prompt,
        )
    attempt_prompt = base_prompt

    retry_delay_seconds = _get_email_retry_delay_seconds()
    keep_alive = _get_ollama_keep_alive()
    last_error: Exception | None = None
    active_model_name = model_name

    for attempt in range(1, max_attempts + 1):
        try:
            if use_prefix_context_cache:
                used_generate = True
                request_fn = lambda resolved_model_name: _ollama_generate_with_schema(
                    ollama_client=ollama_client,
                    model_name=resolved_model_name,
                    prompt=attempt_prompt,
                    context_tokens=active_context_tokens,
                    keep_alive=keep_alive,
                )
            else:
                used_generate = False
                request_fn = lambda resolved_model_name: _ollama_chat_with_schema(
                    ollama_client=ollama_client,
                    model_name=resolved_model_name,
                    prompt=attempt_prompt,
                    keep_alive=keep_alive,
                )

            response, active_model_name = _invoke_with_model_fallback(
                request_fn=request_fn,
                model_name=model_name,
                fallback_warning="Configured OLLAMA_MODEL '%s' is unavailable. Retrying with fallback '%s'.",
            )

            if used_generate:
                content = _extract_ollama_generate_content(response)
                next_context_tokens = _extract_ollama_context_tokens(response)
            else:
                content = _extract_ollama_message_content(response)
                next_context_tokens = None

            structured_output = _parse_structured_email_json(content)

            if attempt > 1:
                logger.info("Email generation recovered on retry attempt %s/%s.", attempt, max_attempts)

            if next_context_tokens is not None:
                active_context_tokens = next_context_tokens

            prefix_context_requested = bool(use_prefix_context_cache and used_generate)
            prefix_context_returned = next_context_tokens is not None

            return {
                "structured": structured_output,
                "full_text": _extract_email_text(structured_output),
                "usage": _extract_ollama_usage(response),
                "context_tokens": active_context_tokens,
                "prefix_context_requested": prefix_context_requested,
                "prefix_context_returned": prefix_context_returned,
                "model_name": active_model_name,
            }
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break

            attempt_prompt = (
                f"{base_prompt}\n\n"
                "IMPORTANT: Your previous output was invalid or truncated JSON. "
                "Return ONLY one complete valid JSON object that matches the schema exactly. "
                "Do not include markdown, commentary, or partial output."
            )

            logger.warning(
                "Email generation failed on attempt %s/%s (%s). Retrying in %.1fs...",
                attempt,
                max_attempts,
                exc,
                retry_delay_seconds,
            )
            if retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)

    raise RuntimeError(
        f"Email generation failed after {max_attempts} attempts: {last_error}"
    ) from last_error


def email_generation(client_instance, df, person_context=None):
    """Generate a single email using Ollama structured output."""
    ollama_client = client_instance or globals().get("client")
    output = _generate_email_internal(
        ollama_client,
        df,
        person_context,
        build_email_prompt,
        use_prefix_context_cache=False,
    )
    return output["structured"]


def email_chain_generation(
    client,
    df,
    person_context,
    use_prefix_context_cache: bool | None = None,
    require_prefix_context_cache: bool | None = None,
    max_generation_retries: int | None = None,
):
    """
    Generate main email + 4 follow-ups in a chain.
    Uses reusable Ollama context tokens when available; otherwise falls back
    to previous-email prompt context for continuity.

    Args:
        client: Ollama client instance
        df: Person data DataFrame/dict
        person_context: Context about the person/campaign

    Returns:
        List of dicts with type, description, structured_output, and full_text
    """
    if client is None:
        raise RuntimeError(
            "Ollama client is not configured. Install ollama and set OLLAMA_HOST/OLLAMA_API_KEY as needed."
        )

    row_dict = _record_to_dict(df)

    logger.info("=" * 80)
    logger.info("Starting Email Chain Generation")
    logger.info(f"Person: {row_dict.get('first_name', 'N/A')} {row_dict.get('last_name', 'N/A')}")
    logger.info(f"Company: {row_dict.get('company_name', 'N/A')}")
    logger.info("=" * 80)

    results = []
    previous_email_text = None
    context_tokens: list[int] | None = None
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_tokens = 0
    total_prefix_context_tokens = 0
    iteration = 0

    if use_prefix_context_cache is None:
        use_prefix_context_cache = _is_prefix_context_cache_enabled()

    if require_prefix_context_cache is None:
        require_prefix_context_cache = _is_prefix_context_cache_required()

    logger.info("Prefix context cache requested: %s", use_prefix_context_cache)
    logger.info("Prefix context cache required: %s", require_prefix_context_cache)
    logger.info("Prefix cache stream mode: %s", _is_prefix_cache_stream_enabled())
    logger.info("Resolved email model: %s", _get_ollama_model_name())

    effective_use_prefix_context_cache = bool(use_prefix_context_cache)

    for prompt_fn, email_type, description in EMAIL_SEQUENCE:
        iteration += 1
        logger.info(f"\n[ITERATION {iteration}/5] Generating: {description}")

        prompt_override = None
        if effective_use_prefix_context_cache and iteration > 1:
            prompt_override = _build_cache_mode_iteration_prompt(
                email_type=email_type,
                description=description,
            )
            logger.info("  Prompt Mode: prefix-cache delta")
        else:
            logger.info("  Prompt Mode: full prompt")

        output = _generate_email_internal(
            ollama_client=client,
            df=df,
            person_context=person_context,
            prompt_fn=prompt_fn,
            previous_email=previous_email_text,
            context_tokens=context_tokens,
            use_prefix_context_cache=effective_use_prefix_context_cache,
            prompt_override=prompt_override,
            max_attempts=max_generation_retries,
        )

        if effective_use_prefix_context_cache and not output.get("prefix_context_returned", False):
            logger.info("  Prefix context not returned by provider on this iteration.")

        if output.get("context_tokens") is not None:
            context_tokens = output["context_tokens"]

        cache_efficiency = _calculate_cache_efficiency(output["usage"])

        total_input_tokens += output["usage"]["input_tokens"]
        total_output_tokens += output["usage"]["output_tokens"]
        total_cache_tokens += output["usage"]["cache_read_input_tokens"]
        total_prefix_context_tokens += output["usage"].get("prefix_context_tokens", 0)

        logger.info(f"  Type: {email_type}")
        logger.info("  Token Usage:")
        logger.info(f"    - Input Tokens: {output['usage']['input_tokens']}")
        logger.info(f"    - Output Tokens: {output['usage']['output_tokens']}")
        logger.info(f"    - Prefix Context Tokens: {output['usage'].get('prefix_context_tokens', 0)}")
        logger.info(f"    - Cache Creation Tokens: {output['usage']['cache_creation_input_tokens']}")
        logger.info(f"    - Cache Read Tokens: {output['usage']['cache_read_input_tokens']}")
        logger.info("  Cache Efficiency:")
        logger.info(f"    - Cache Hit Rate: {cache_efficiency['cache_hit_rate']}")
        logger.info(f"    - New Tokens Processed: {cache_efficiency['new_input_tokens']}")

        results.append(
            {
                "type": email_type,
                "description": description,
                "structured_output": output["structured"],
                "full_text": output["full_text"],
            }
        )

        previous_email_text = output["full_text"]

    logger.info("\n" + "=" * 80)
    logger.info("Email Chain Generation Complete - Final Summary")
    logger.info("=" * 80)
    logger.info(f"Total Emails Generated: {len(results)}")
    logger.info(f"Total Input Tokens: {total_input_tokens}")
    logger.info(f"Total Output Tokens: {total_output_tokens}")
    logger.info(f"Total Prefix Context Tokens: {total_prefix_context_tokens}")
    logger.info(f"Total Cache Read Tokens (provider reported): {total_cache_tokens}")
    logger.info(f"Total Tokens: {total_input_tokens + total_output_tokens}")
    logger.info("=" * 80 + "\n")

    return results


def save_email_chain_results(results, output_file=None) -> tuple:
    """
    Save email chain results to Excel file with structured components.
    Creates 15 columns: 3 components (intro, value prop, CTA) × 5 emails (main + 4 FUs).
    If file exists, appends new records. Otherwise creates new file.
    
    Args:
        results: Output from email_chain_generation()
        output_file: Optional custom output path. Defaults to instantly/email_chain_output.xlsx
        
    Returns:
        Tuple of (DataFrame, file_path)
    """
    if not output_file:
        output_file = "instantly/email_chain_output.xlsx"

    logger.info(f"Generating Excel report with email components...")

    # Build column structure: 3 components × 5 emails = 15 columns
    email_data = {}
    
    for result in results:
        email_type = result["type"]
        structured = result["structured_output"]
        
        email_data[f"{email_type}_introduction"] = structured["introduction"]
        email_data[f"{email_type}_value_proposition"] = structured["value_proposition"]
        email_data[f"{email_type}_call_to_action"] = structured["call_to_action"]
    
    # Create DataFrame with single row
    df_new = pd.DataFrame([email_data])
    
    # Reorder columns for clarity: main, followup_1, followup_2, followup_3, followup_4
    column_order = [
        "main_email_introduction", "main_email_value_proposition", "main_email_call_to_action",
        "followup_1_introduction", "followup_1_value_proposition", "followup_1_call_to_action",
        "followup_2_introduction", "followup_2_value_proposition", "followup_2_call_to_action",
        "followup_3_introduction", "followup_3_value_proposition", "followup_3_call_to_action",
        "followup_4_introduction", "followup_4_value_proposition", "followup_4_call_to_action",
    ]
    
    df_new = df_new[column_order]
    
    # Format Excel file path
    excel_file = output_file.replace(".json", ".xlsx") if ".json" in output_file else output_file
    
    # Check if file exists and append or create
    if os.path.exists(excel_file):
        logger.info(f"Excel file exists. Appending new records to: {excel_file}")
        df_existing = pd.read_excel(excel_file, sheet_name="Email Components")
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
        logger.info(f"Appended 1 new record. Total rows after append: {len(df_final)}")
    else:
        df_final = df_new
        logger.info(f"Creating new Excel file: {excel_file}")
    
    # Save to Excel
    df_final.to_excel(excel_file, index=False, sheet_name="Email Components")
    
    logger.info(f"Excel file saved: {excel_file}")
    logger.info(f"Columns (15): {', '.join(column_order)}")
    logger.info(f"Total rows in file: {len(df_final)}")
    
    return df_final, excel_file
