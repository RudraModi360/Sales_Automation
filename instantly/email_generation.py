import os
import importlib
import json
import logging
from datetime import datetime
from pydantic import BaseModel
from dotenv import load_dotenv
from groq import Groq
import pandas as pd
from .prompts import (
    email_prompt as build_email_prompt,
    followup_1_prompt,
    followup_2_prompt,
    followup_3_prompt,
    followup_4_prompt,
    company_data,
    sender_info,
)

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('email_chain_generation.log')
    ]
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

Provide a brief summary (max 500 words) about the person and company. Be concise with token usage."""
            }
        ],
    )
    return completion.choices[0].message.content


def _validate_parsed_output(response):
    """Extract and validate structured output from API response."""
    if not getattr(response, "parsed_output", None):
        raise RuntimeError("No structured output returned by Anthropic parse API")
    return EmailStructuredOutput.model_validate(response.parsed_output).model_dump()


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


def _build_email_messages(prompt_fn, df, person_context, use_caching=False, previous_email=None):
    """Build API messages for email generation, with optional caching."""
    current_prompt = _build_email_prompt_with_context(prompt_fn, df, person_context, previous_email)
    
    if use_caching:
        static_context = f"""STATIC CONTEXT FOR ALL EMAILS (CACHED):

Person Data:
{json.dumps(df.to_dict(), indent=2)}

Current Person/Campaign Context:
{person_context}

Sender's Company Information:
{company_data()}

Sender's Information:
{sender_info()}"""
        
        return [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": static_context, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": current_prompt},
                    {"type": "text", "text": f"Format Instructions (CACHED):\n{STRUCTURED_OUTPUT_FORMAT}", "cache_control": {"type": "ephemeral"}},
                ],
            }
        ]
    else:
        prompt = current_prompt + "\n\n" + STRUCTURED_OUTPUT_FORMAT
        return [{"role": "user", "content": prompt}]


def _generate_email_internal(anthropic_client, df, person_context, prompt_fn, use_caching=False, previous_email=None) -> dict:
    """Unified internal function for email generation with optional caching."""
    if anthropic_client is None:
        raise RuntimeError("Anthropic client is not configured. Set ANTHROPIC_API_KEY and install anthropic.")
    
    messages = _build_email_messages(prompt_fn, df, person_context, use_caching, previous_email)
    
    response = anthropic_client.messages.parse(
        model=os.getenv("ANTHROPIC_MODEL", DEFAULT_MODEL),
        max_tokens=DEFAULT_MAX_TOKENS,
        messages=messages,
        output_format=EmailStructuredOutput,
    )
    
    structured_output = _validate_parsed_output(response)
    
    return {
        "structured": structured_output,
        "full_text": _extract_email_text(structured_output),
        "usage": {
            "input_tokens": response.usage.input_tokens,
            "cache_creation_input_tokens": getattr(response.usage, "cache_creation_input_tokens", 0),
            "cache_read_input_tokens": getattr(response.usage, "cache_read_input_tokens", 0),
            "output_tokens": response.usage.output_tokens,
        },
    }


def email_generation(client_instance, df, person_context=None):
    """Generate single email without caching."""
    anthropic_client = client_instance or globals().get("client")
    output = _generate_email_internal(anthropic_client, df, person_context, build_email_prompt, use_caching=False)
    return output["structured"]


# ==================== CHAIN GENERATION WITH PROMPT CACHING ====================


def email_chain_generation(client, df, person_context):
    """
    Generate main email + 4 follow-ups in a chain with prompt prefix caching.
    Each email uses the previous email as context for continuity.
    Logs telemetry data after each iteration.
    
    Args:
        client: Anthropic client instance
        df: Person data DataFrame/dict
        person_context: Context about the person/campaign
        
    Returns:
        List of dicts with type, description, structured_output, full_text, token_usage, and cache_efficiency
    """
    if client is None:
        raise RuntimeError("Anthropic client is not configured. Set ANTHROPIC_API_KEY and install anthropic.")

    logger.info("=" * 80)
    logger.info("Starting Email Chain Generation")
    logger.info(f"Person: {df.get('first_name', 'N/A')} {df.get('last_name', 'N/A')}")
    logger.info(f"Company: {df.get('company_name', 'N/A')}")
    logger.info("=" * 80)

    results = []
    previous_email_text = None
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_tokens = 0
    iteration = 0

    for prompt_fn, email_type, description in EMAIL_SEQUENCE:
        iteration += 1
        logger.info(f"\n[ITERATION {iteration}/5] Generating: {description}")
        
        output = _generate_email_internal(
            anthropic_client=client,
            df=df,
            person_context=person_context,
            prompt_fn=prompt_fn,
            use_caching=True,
            previous_email=previous_email_text,
        )

        cache_efficiency = _calculate_cache_efficiency(output["usage"])
        
        total_input_tokens += output["usage"]["input_tokens"]
        total_output_tokens += output["usage"]["output_tokens"]
        total_cache_tokens += output["usage"]["cache_read_input_tokens"]
        
        logger.info(f"  Type: {email_type}")
        logger.info(f"  Token Usage:")
        logger.info(f"    - Input Tokens: {output['usage']['input_tokens']}")
        logger.info(f"    - Output Tokens: {output['usage']['output_tokens']}")
        logger.info(f"    - Cache Creation Tokens: {output['usage']['cache_creation_input_tokens']}")
        logger.info(f"    - Cache Read Tokens (90% savings): {output['usage']['cache_read_input_tokens']}")
        logger.info(f"  Cache Efficiency:")
        logger.info(f"    - Cache Hit Rate: {cache_efficiency['cache_hit_rate']}")
        logger.info(f"    - New Tokens Processed: {cache_efficiency['new_input_tokens']}")
        logger.info(f"  Estimated Cost Savings: {output['usage']['cache_read_input_tokens'] * 0.9:.0f} tokens (~90% discount)")

        results.append({
            "type": email_type,
            "description": description,
            "structured_output": output["structured"],
            "full_text": output["full_text"],
            "token_usage": {
                "input_tokens": output["usage"]["input_tokens"],
                "cache_creation_input_tokens": output["usage"]["cache_creation_input_tokens"],
                "cache_read_input_tokens": output["usage"]["cache_read_input_tokens"],
                "output_tokens": output["usage"]["output_tokens"],
            },
            "cache_efficiency": cache_efficiency,
        })

        previous_email_text = output["full_text"]

    logger.info("\n" + "=" * 80)
    logger.info("Email Chain Generation Complete - Final Summary")
    logger.info("=" * 80)
    logger.info(f"Total Emails Generated: {len(results)}")
    logger.info(f"Total Input Tokens: {total_input_tokens}")
    logger.info(f"Total Output Tokens: {total_output_tokens}")
    logger.info(f"Total Cache Read Tokens (saved): {total_cache_tokens}")
    logger.info(f"Total Tokens with Cache: {total_input_tokens + total_output_tokens}")
    logger.info(f"Estimated Tokens without Cache: {total_input_tokens + total_cache_tokens + total_output_tokens}")
    logger.info(f"Total Savings: ~{int(total_cache_tokens * 0.9)} tokens (90% discount on cache reads)")
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
