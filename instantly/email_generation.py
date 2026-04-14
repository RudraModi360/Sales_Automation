import importlib
import json
import logging
import os

import pandas as pd
from dotenv import load_dotenv
from groq import Groq
from pydantic import BaseModel

try:
    from .llm_runtime import BaseLLMProvider, LLMSessionState, build_provider, get_session_state
    from .prompts import (
        company_data,
        email_prompt as build_email_prompt,
        followup_1_prompt,
        followup_2_prompt,
        followup_3_prompt,
        followup_4_prompt,
        sender_info,
    )
except ImportError:
    from llm_runtime import BaseLLMProvider, LLMSessionState, build_provider, get_session_state
    from prompts import (
        company_data,
        email_prompt as build_email_prompt,
        followup_1_prompt,
        followup_2_prompt,
        followup_3_prompt,
        followup_4_prompt,
        sender_info,
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
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_MAX_TOKENS = 2048

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


def _build_anthropic_client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    try:
        anthropic_module = importlib.import_module("anthropic")
    except ModuleNotFoundError:
        return None

    return anthropic_module.Anthropic(api_key=api_key)


client = _build_anthropic_client()

client_groq = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)


class EmailStructuredOutput(BaseModel):
    introduction: str
    value_proposition: str
    call_to_action: str


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


def _record_to_dict(record) -> dict:
    if isinstance(record, dict):
        return record

    if isinstance(record, pd.Series):
        return record.to_dict()

    if hasattr(record, "to_dict"):
        as_dict = record.to_dict()
        if isinstance(as_dict, dict):
            return as_dict

    raise ValueError("record must be a dict-like object or pandas Series")


def _build_email_prompt_with_context(prompt_fn, df, person_context, previous_email=None):
    """Build prompt for email generation, optionally including previous email context."""
    current_prompt = prompt_fn(df, person_context)

    if previous_email:
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
    """Calculate cache hit metrics from token usage."""
    total_input = int(usage_stats.get("input_tokens", 0) or 0)
    cache_read = int(usage_stats.get("cache_read_input_tokens", 0) or 0)
    if cache_read == 0:
        # Ollama does not return explicit cache-read tokens. Use incoming context length
        # as a best-effort proxy when prefix context is actually reused.
        cache_read = int(usage_stats.get("context_tokens_in", 0) or 0)
    new_tokens = max(0, total_input - cache_read)

    cache_hit_rate = f"{(cache_read / total_input * 100):.1f}%" if total_input > 0 else "0%"

    return {
        "cache_creation_tokens": int(usage_stats.get("cache_creation_input_tokens", 0) or 0),
        "cache_read_tokens": cache_read,
        "new_input_tokens": new_tokens,
        "total_input_tokens": total_input,
        "cache_hit_rate": cache_hit_rate,
    }


def _build_static_context(df, person_context):
    """Build static context block reused across a sequence for cache-friendly providers."""
    record = _record_to_dict(df)

    return f"""STATIC CONTEXT FOR ALL EMAILS (CACHED):

Person Data:
{json.dumps(record, indent=2)}

Current Person/Campaign Context:
{person_context}

Sender's Company Information:
{company_data()}

Sender's Information:
{sender_info()}"""


def _resolve_provider(provider_or_client=None, provider_name=None) -> BaseLLMProvider:
    if isinstance(provider_or_client, BaseLLMProvider):
        return provider_or_client

    normalized = (provider_name or os.getenv("LLM_PROVIDER") or "").strip().lower()
    if normalized:
        return build_provider(provider_name=normalized, anthropic_client=provider_or_client)

    if provider_or_client is not None:
        return build_provider(provider_name="anthropic", anthropic_client=provider_or_client)

    return build_provider(provider_name="anthropic", anthropic_client=globals().get("client"))


def _generate_email_internal(
    provider_or_client,
    df,
    person_context,
    prompt_fn,
    session_state: LLMSessionState,
    use_caching=False,
    previous_email=None,
    provider_name=None,
) -> dict:
    """Unified internal function for provider-agnostic email generation with optional caching."""
    provider = _resolve_provider(provider_or_client=provider_or_client, provider_name=provider_name)
    current_prompt = _build_email_prompt_with_context(prompt_fn, df, person_context, previous_email)

    # Prompt templates already embed person/company/sender context. For Ollama,
    # sending an additional static block can inflate prompt tokens heavily,
    # especially when context-token reuse is unavailable.
    static_context = None
    if use_caching and provider.provider_name == "anthropic":
        static_context = _build_static_context(df, person_context)

    output = provider.generate_structured(
        prompt=current_prompt,
        output_model=EmailStructuredOutput,
        format_instructions=STRUCTURED_OUTPUT_FORMAT,
        session_state=session_state,
        use_cache=use_caching,
        static_context=static_context,
        max_tokens=DEFAULT_MAX_TOKENS,
    )

    structured_output = output.structured_output
    full_text = _extract_email_text(structured_output)

    # Persist conversational state independent of provider.
    session_state.append_turn(current_prompt, full_text)

    return {
        "structured": structured_output,
        "full_text": full_text,
        "usage": output.usage,
        "provider": provider.provider_name,
        "session_id": session_state.session_id,
    }


def email_generation(
    client_instance,
    df,
    person_context=None,
    provider_name: str = None,
    session_id: str = None,
    use_caching: bool = False,
):
    """Generate a single email with optional persistent session state and provider selection."""
    session_state = get_session_state(session_id=session_id)
    output = _generate_email_internal(
        provider_or_client=client_instance or globals().get("client"),
        df=df,
        person_context=person_context,
        prompt_fn=build_email_prompt,
        session_state=session_state,
        use_caching=use_caching,
        previous_email=None,
        provider_name=provider_name,
    )
    return output["structured"]


# ==================== CHAIN GENERATION WITH SESSION STATE ====================


def email_chain_generation(
    client,
    df,
    person_context,
    provider_name: str = None,
    session_id: str = None,
    reset_session: bool = False,
):
    """
    Generate main email + 4 follow-ups in a chain with provider-managed context caching.
    Each email uses the previous email as context for continuity while also persisting
    session state in a dedicated runtime store.

    Args:
        client: Provider object or provider-native client instance
        df: Person data dict or pandas Series
        person_context: Context about the person/campaign
        provider_name: Explicit provider override (anthropic or ollama)
        session_id: Optional persistent session identifier
        reset_session: Clears persisted state for provided session_id before running

    Returns:
        List of dicts with type, description, structured_output, full_text, token_usage,
        cache_efficiency, provider, and session_id
    """
    session_state = get_session_state(session_id=session_id)
    if reset_session:
        session_state.clear()

    provider = _resolve_provider(provider_or_client=client, provider_name=provider_name)

    logger.info("=" * 80)
    logger.info("Starting Email Chain Generation")
    logger.info(f"Provider: {provider.provider_name}")
    logger.info(f"Session: {session_state.session_id}")
    logger.info(f"Person: {df.get('first_name', 'N/A')} {df.get('last_name', 'N/A')}")
    logger.info(f"Company: {df.get('company_name', 'N/A')}")
    logger.info("=" * 80)

    results = []
    previous_email_text = None
    for item in reversed(session_state.message_history):
        if item.get("role") == "assistant":
            previous_email_text = item.get("content")
            break
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_tokens = 0
    iteration = 0

    for prompt_fn, email_type, description in EMAIL_SEQUENCE:
        iteration += 1
        logger.info(f"\n[ITERATION {iteration}/5] Generating: {description}")

        output = _generate_email_internal(
            provider_or_client=provider,
            df=df,
            person_context=person_context,
            prompt_fn=prompt_fn,
            session_state=session_state,
            use_caching=True,
            previous_email=previous_email_text,
            provider_name=provider_name,
        )

        cache_efficiency = _calculate_cache_efficiency(output["usage"])

        total_input_tokens += int(output["usage"].get("input_tokens", 0) or 0)
        total_output_tokens += int(output["usage"].get("output_tokens", 0) or 0)
        total_cache_tokens += int(output["usage"].get("cache_read_input_tokens", 0) or 0)

        logger.info(f"  Type: {email_type}")
        logger.info("  Token Usage:")
        logger.info(f"    - Input Tokens: {output['usage'].get('input_tokens', 0)}")
        logger.info(f"    - Output Tokens: {output['usage'].get('output_tokens', 0)}")
        logger.info(
            f"    - Cache Creation Tokens: {output['usage'].get('cache_creation_input_tokens', 0)}"
        )
        logger.info(
            f"    - Cache Read Tokens (90% savings): {output['usage'].get('cache_read_input_tokens', 0)}"
        )
        logger.info("  Cache Efficiency:")
        logger.info(f"    - Cache Hit Rate: {cache_efficiency['cache_hit_rate']}")
        logger.info(f"    - New Tokens Processed: {cache_efficiency['new_input_tokens']}")
        if output["provider"] == "ollama":
            logger.info(
                "    - Prefix Cache Reused: "
                f"{bool(output['usage'].get('prefix_cache_reused', False))}"
            )
            logger.info(
                "    - Context Tokens In (proxy for reused tokens): "
                f"{int(output['usage'].get('context_tokens_in', 0) or 0)}"
            )
            logger.info(
                "    - Prompt Chars Sent: "
                f"{int(output['usage'].get('prompt_char_count', 0) or 0)}"
            )

        results.append(
            {
                "type": email_type,
                "description": description,
                "structured_output": output["structured"],
                "full_text": output["full_text"],
                "token_usage": output["usage"],
                "cache_efficiency": cache_efficiency,
                "provider": output["provider"],
                "session_id": output["session_id"],
            }
        )

        previous_email_text = output["full_text"]

    logger.info("\n" + "=" * 80)
    logger.info("Email Chain Generation Complete - Final Summary")
    logger.info("=" * 80)
    logger.info(f"Total Emails Generated: {len(results)}")
    logger.info(f"Total Input Tokens: {total_input_tokens}")
    logger.info(f"Total Output Tokens: {total_output_tokens}")
    logger.info(f"Total Cache Read Tokens (saved): {total_cache_tokens}")
    logger.info(f"Total Tokens with Cache: {total_input_tokens + total_output_tokens}")
    logger.info(
        "Estimated Tokens without Cache: "
        f"{total_input_tokens + total_cache_tokens + total_output_tokens}"
    )
    logger.info(f"Total Savings: ~{int(total_cache_tokens * 0.9)} tokens (90% discount on cache reads)")
    logger.info("=" * 80 + "\n")

    return results


def save_email_chain_results(results, output_file=None) -> tuple:
    """
    Save email chain results to Excel file with structured components.
    Creates 15 columns: 3 components (intro, value prop, CTA) x 5 emails (main + 4 FUs).
    If file exists, appends new records. Otherwise creates new file.

    Args:
        results: Output from email_chain_generation()
        output_file: Optional custom output path. Defaults to instantly/email_chain_output.xlsx

    Returns:
        Tuple of (DataFrame, file_path)
    """
    if not output_file:
        output_file = "instantly/email_chain_output.xlsx"

    logger.info("Generating Excel report with email components...")

    # Build column structure: 3 components x 5 emails = 15 columns
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
        "main_email_introduction",
        "main_email_value_proposition",
        "main_email_call_to_action",
        "followup_1_introduction",
        "followup_1_value_proposition",
        "followup_1_call_to_action",
        "followup_2_introduction",
        "followup_2_value_proposition",
        "followup_2_call_to_action",
        "followup_3_introduction",
        "followup_3_value_proposition",
        "followup_3_call_to_action",
        "followup_4_introduction",
        "followup_4_value_proposition",
        "followup_4_call_to_action",
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
