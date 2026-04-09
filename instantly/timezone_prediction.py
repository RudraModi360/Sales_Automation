import os
import re
from typing import Optional

import anthropic
from dotenv import load_dotenv

load_dotenv()

# Complete timezone list from Instantly.ai API reference
VALID_TIMEZONES = {
    "Etc/GMT+12", "Etc/GMT+11", "Etc/GMT+10", "America/Anchorage", "America/Dawson",
    "America/Creston", "America/Chihuahua", "America/Boise", "America/Belize",
    "America/Chicago", "America/Bahia_Banderas", "America/Regina", "America/Bogota",
    "America/Detroit", "America/Indiana/Marengo", "America/Caracas", "America/Asuncion",
    "America/Glace_Bay", "America/Campo_Grande", "America/Anguilla", "America/Santiago",
    "America/St_Johns", "America/Sao_Paulo", "America/Argentina/La_Rioja",
    "America/Araguaina", "America/Godthab", "America/Montevideo", "America/Bahia",
    "America/Noronha", "America/Scoresbysund", "Atlantic/Cape_Verde", "Africa/Casablanca",
    "America/Danmarkshavn", "Europe/Isle_of_Man", "Atlantic/Canary", "Africa/Abidjan",
    "Arctic/Longyearbyen", "Europe/Belgrade", "Africa/Ceuta", "Europe/Sarajevo",
    "Africa/Algiers", "Africa/Windhoek", "Asia/Nicosia", "Asia/Beirut", "Africa/Cairo",
    "Asia/Damascus", "Europe/Bucharest", "Africa/Blantyre", "Europe/Helsinki",
    "Europe/Istanbul", "Asia/Jerusalem", "Africa/Tripoli", "Asia/Amman", "Asia/Baghdad",
    "Europe/Kaliningrad", "Asia/Aden", "Africa/Addis_Ababa", "Europe/Kirov",
    "Europe/Astrakhan", "Asia/Tehran", "Asia/Dubai", "Asia/Baku", "Indian/Mahe",
    "Asia/Tbilisi", "Asia/Yerevan", "Asia/Kabul", "Antarctica/Mawson",
    "Asia/Yekateburk", "Asia/Karachi", "Asia/Kolkata", "Asia/Colombo",
    "Asia/Kathmandu", "Antarctica/Vostok", "Asia/Dhaka", "Asia/Rangoon",
    "Antarctica/Davis", "Asia/Novokuznetsk", "Asia/Hong_Kong", "Asia/Krasnoyarsk",
    "Asia/Brunei", "Australia/Perth", "Asia/Taipei", "Asia/Choibalsan", "Asia/Irkutsk",
    "Asia/Dili", "Asia/Pyongyang", "Australia/Adelaide", "Australia/Darwin",
    "Australia/Brisbane", "Australia/Melbourne", "Antarctica/DumontDUrville",
    "Australia/Currie", "Asia/Chita", "Antarctica/Macquarie", "Asia/Sakhalin",
    "Pacific/Auckland", "Etc/GMT-12", "Pacific/Fiji", "Asia/Anadyr", "Asia/Kamchatka",
    "Etc/GMT-13", "Pacific/Apia"
}

VALID_TIMEZONE_LIST_PROMPT = ", ".join(sorted(VALID_TIMEZONES))

# Common aliases that should be normalized into Instantly-allowed values.
TIMEZONE_ALIASES = {
    "asia/calcutta": "Asia/Kolkata",
    "etc/utc": "Africa/Abidjan",
    "utc": "Africa/Abidjan",
    "gmt": "Africa/Abidjan",
    "z": "Africa/Abidjan",
}

# Map fractional UTC offsets to nearest valid timezone in Instantly's allowed list.
OFFSET_TO_VALID_TIMEZONE = {
    -3.5: "America/St_Johns",
    3.5: "Asia/Tehran",
    4.5: "Asia/Kabul",
    5.5: "Asia/Kolkata",
    5.75: "Asia/Kathmandu",
    9.5: "Australia/Darwin",
}

# Deterministic fallback map for high-frequency geography contexts.
CONTEXT_TIMEZONE_FALLBACKS = {
    "india": "Asia/Kolkata",
    "united states": "America/Detroit",
    "usa": "America/Detroit",
    "u.s.": "America/Detroit",
    "new york": "America/Detroit",
    "canada": "America/Detroit",
    "uk": "Europe/Isle_of_Man",
    "united kingdom": "Europe/Isle_of_Man",
    "england": "Europe/Isle_of_Man",
    "ireland": "Europe/Isle_of_Man",
    "uae": "Asia/Dubai",
    "dubai": "Asia/Dubai",
    "australia": "Australia/Melbourne",
    "new zealand": "Pacific/Auckland",
    "singapore": "Asia/Hong_Kong",
    "hong kong": "Asia/Hong_Kong",
    "pakistan": "Asia/Karachi",
    "bangladesh": "Asia/Dhaka",
    "sri lanka": "Asia/Colombo",
    "nepal": "Asia/Kathmandu",
}

ETC_GMT_OFFSET_PATTERN = re.compile(r"^Etc/GMT([+-])(\d{1,2}(?:\.\d+)?)$", re.IGNORECASE)
UTC_GMT_OFFSET_PATTERN = re.compile(r"^(?:UTC|GMT)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?$", re.IGNORECASE)


def _fallback_timezone_from_context(context: str) -> Optional[str]:
    context_lower = (context or "").lower()
    for keyword, timezone in CONTEXT_TIMEZONE_FALLBACKS.items():
        if keyword in context_lower:
            return timezone
    return None


def _normalize_offset_timezone(raw_timezone: str) -> Optional[str]:
    etc_match = ETC_GMT_OFFSET_PATTERN.match(raw_timezone)
    if etc_match:
        sign_symbol, hours_text = etc_match.groups()
        signed_offset = float(hours_text)
        if sign_symbol == "-":
            signed_offset = -signed_offset

        # Etc/GMT has reversed sign semantics compared to UTC offsets.
        utc_offset = -signed_offset
        mapped_timezone = OFFSET_TO_VALID_TIMEZONE.get(round(utc_offset, 2))
        if mapped_timezone:
            return mapped_timezone

    offset_match = UTC_GMT_OFFSET_PATTERN.match(raw_timezone)
    if offset_match:
        sign_symbol, hours_text, minutes_text = offset_match.groups()
        hours = int(hours_text)
        minutes = int(minutes_text) if minutes_text else 0
        utc_offset = hours + (minutes / 60)
        if sign_symbol == "-":
            utc_offset = -utc_offset

        mapped_timezone = OFFSET_TO_VALID_TIMEZONE.get(round(utc_offset, 2))
        if mapped_timezone:
            return mapped_timezone

    return None


def normalize_timezone(
    timezone: str,
    context: str = "",
    allow_context_fallback: bool = False,
) -> Optional[str]:
    if not isinstance(timezone, str):
        return None

    candidate = timezone.strip()
    if not candidate:
        return None

    if candidate in VALID_TIMEZONES:
        return candidate

    alias_match = TIMEZONE_ALIASES.get(candidate.lower())
    if alias_match in VALID_TIMEZONES:
        return alias_match

    offset_match = _normalize_offset_timezone(candidate)
    if offset_match in VALID_TIMEZONES:
        return offset_match

    if allow_context_fallback:
        context_fallback = _fallback_timezone_from_context(context)
        if context_fallback in VALID_TIMEZONES:
            return context_fallback

    return None


def is_valid_timezone(timezone: str) -> bool:
    """
    Validate that timezone is explicitly allowed by Instantly.
    
    Args:
        timezone: Timezone string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    return isinstance(timezone, str) and timezone.strip() in VALID_TIMEZONES


def predict_timezone(
    context: str,
    max_retries: int = 3,
    previous_predictions: Optional[list] = None
) -> dict:
    """
    Predict timezone using Claude with strict validation against Instantly-supported values.
    
    Args:
        context: Description/context about the timezone (e.g., "User is in Mumbai, India")
        max_retries: Maximum retry attempts (default 3)
        previous_predictions: List of previous failed predictions for context (for fallback)
        
    Returns:
        dict: {
            "timezone": str,
            "confidence": str ("high", "medium", "low"),
            "reasoning": str,
            "is_valid": bool,
            "attempt": int,
            "success": bool,
            "error_message": Optional[str]
        }
    """
    
    context = str(context or "").strip() or "unknown"
    max_retries = max(1, int(max_retries))
    attempted_predictions = [
        str(pred).strip() for pred in (previous_predictions or []) if str(pred).strip()
    ]

    do_not_repeat_note = ""
    if attempted_predictions:
        do_not_repeat_note = (
            "\n\nRejected attempts (do not repeat): "
            + ", ".join(attempted_predictions)
        )

    system_prompt = f"""You are a timezone prediction expert.

CRITICAL RULES:
1. Return EXACTLY one timezone from the allowed list below.
2. Do NOT return UTC/GMT offsets (for example: UTC+05:30, GMT+5:30, Etc/GMT-5.5).
3. Do NOT invent new timezone names.
4. Prefer major city timezone for the given context.

ALLOWED TIMEZONE LIST:
{VALID_TIMEZONE_LIST_PROMPT}{do_not_repeat_note}"""

    client = anthropic.Anthropic()
    model_name = os.getenv("ANTHROPIC_LIGHT_MODEL") or "claude-3-5-haiku-latest"
    last_error_message = ""

    for attempt in range(1, max_retries + 1):
        try:
            response = client.messages.create(
                model=model_name,
                max_tokens=500,
                system=system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": f"Predict the timezone for: {context}",
                    }
                ],
                tools=[
                    {
                        "name": "timezone_prediction",
                        "description": "Predict a timezone from the allowed list",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "timezone": {
                                    "type": "string",
                                    "description": "Timezone value selected from the allowed list",
                                },
                                "confidence": {
                                    "type": "string",
                                    "enum": ["high", "medium", "low"],
                                    "description": "Confidence level of prediction",
                                },
                                "reasoning": {
                                    "type": "string",
                                    "description": "Short reasoning for timezone choice",
                                },
                            },
                            "required": ["timezone", "confidence", "reasoning"],
                        },
                    }
                ],
            )
        except anthropic.APIError as exc:
            last_error_message = str(exc)
            if attempt < max_retries:
                continue
            break

        tool_result = None
        for content_block in response.content:
            if content_block.type == "tool_use":
                tool_result = content_block.input
                break

        if not tool_result:
            last_error_message = "No tool use output received"
            if attempt < max_retries:
                continue
            break

        raw_timezone = str(tool_result.get("timezone", "") or "").strip()
        normalized_timezone = normalize_timezone(
            raw_timezone,
            context=context,
            allow_context_fallback=False,
        )

        if normalized_timezone and is_valid_timezone(normalized_timezone):
            reasoning = str(tool_result.get("reasoning", "") or "").strip()
            if normalized_timezone != raw_timezone:
                reasoning = (
                    f"{reasoning} Normalized '{raw_timezone}' to '{normalized_timezone}'."
                ).strip()

            return {
                "timezone": normalized_timezone,
                "confidence": tool_result.get("confidence", "medium"),
                "reasoning": reasoning,
                "is_valid": True,
                "attempt": attempt,
                "success": True,
                "error_message": None,
            }

        attempted_predictions.append(raw_timezone or "<empty>")
        last_error_message = f"Timezone '{raw_timezone}' is invalid for Instantly"

    fallback_timezone = normalize_timezone(
        "",
        context=context,
        allow_context_fallback=True,
    ) or "Africa/Abidjan"

    return {
        "timezone": fallback_timezone,
        "confidence": "low",
        "reasoning": (
            "Used deterministic fallback timezone after prediction retries "
            "failed validation."
        ),
        "is_valid": True,
        "attempt": max_retries,
        "success": True,
        "error_message": last_error_message or None,
    }


# if __name__ == "__main__":
#     print(predict_timezone("Austria")["timezone"])