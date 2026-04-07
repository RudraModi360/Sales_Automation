import anthropic
import re,os
from typing import Optional, Union
from enum import Enum
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


def is_valid_timezone(timezone: str) -> bool:
    """
    Validate if timezone is in the allowed list or matches Etc/GMT pattern
    
    Args:
        timezone: Timezone string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    # Check if in explicit list
    if timezone in VALID_TIMEZONES:
        return True
    
    # Check if matches Etc/GMT pattern (e.g., Etc/GMT+5, Etc/GMT-8)
    etc_pattern = r'^Etc/GMT[+-]\d{1,2}$'
    if re.match(etc_pattern, timezone):
        return True
    
    return False


def predict_timezone(
    context: str,
    max_retries: int = 3,
    previous_predictions: Optional[list] = None
) -> dict:
    """
    Predict timezone using Claude Haiku 4.5 with structured outputs
    
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
    
    client = anthropic.Anthropic()
    
    # Build the prompt with retry context
    fallback_context = ""
    if previous_predictions:
        fallback_context = f"\n\nPrevious attempts that failed validation: {previous_predictions}\nDo NOT repeat these. Be more specific and accurate."
    
    system_prompt = f"""You are a timezone prediction expert. Given a context/description, predict the most accurate timezone.

CRITICAL RULES:
1. ONLY predict from this exact list of valid timezones OR match the pattern "Etc/GMT±NN"
2. The timezone must be from the Instantly.ai API reference list
3. For countries/regions not explicitly listed, use the Etc/GMT format (e.g., Etc/GMT+5.5 for India regions, Etc/GMT-8 for specific US regions)
4. Consider professional timezone standards for any location
5. When unsure, default to the major city timezone in that region
6. NEVER make up timezone names

VALID TIMEZONE LIST (use EXACTLY as shown):
Etc/GMT+12, Etc/GMT+11, Etc/GMT+10, America/Anchorage, America/Dawson, America/Creston, America/Chihuahua, America/Boise, America/Belize, America/Chicago, America/Bahia_Banderas, America/Regina, America/Bogota, America/Detroit, America/Indiana/Marengo, America/Caracas, America/Asuncion, America/Glace_Bay, America/Campo_Grande, America/Anguilla, America/Santiago, America/St_Johns, America/Sao_Paulo, America/Argentina/La_Rioja, America/Araguaina, America/Godthab, America/Montevideo, America/Bahia, America/Noronha, America/Scoresbysund, Atlantic/Cape_Verde, Africa/Casablanca, America/Danmarkshavn, Europe/Isle_of_Man, Atlantic/Canary, Africa/Abidjan, Arctic/Longyearbyen, Europe/Belgrade, Africa/Ceuta, Europe/Sarajevo, Africa/Algiers, Africa/Windhoek, Asia/Nicosia, Asia/Beirut, Africa/Cairo, Asia/Damascus, Europe/Bucharest, Africa/Blantyre, Europe/Helsinki, Europe/Istanbul, Asia/Jerusalem, Africa/Tripoli, Asia/Amman, Asia/Baghdad, Europe/Kaliningrad, Asia/Aden, Africa/Addis_Ababa, Europe/Kirov, Europe/Astrakhan, Asia/Tehran, Asia/Dubai, Asia/Baku, Indian/Mahe, Asia/Tbilisi, Asia/Yerevan, Asia/Kabul, Antarctica/Mawson, Asia/Yekateburk, Asia/Karachi, Asia/Kolkata, Asia/Colombo, Asia/Kathmandu, Antarctica/Vostok, Asia/Dhaka, Asia/Rangoon, Antarctica/Davis, Asia/Novokuznetsk, Asia/Hong_Kong, Asia/Krasnoyarsk, Asia/Brunei, Australia/Perth, Asia/Taipei, Asia/Choibalsan, Asia/Irkutsk, Asia/Dili, Asia/Pyongyang, Australia/Adelaide, Australia/Darwin, Australia/Brisbane, Australia/Melbourne, Antarctica/DumontDUrville, Australia/Currie, Asia/Chita, Antarctica/Macquarie, Asia/Sakhalin, Pacific/Auckland, Etc/GMT-12, Pacific/Fiji, Asia/Anadyr, Asia/Kamchatka, Etc/GMT-13, Pacific/Apia
{fallback_context}"""

    for attempt in range(1, max_retries + 1):
        try:
            response = client.messages.create(
                model=os.getenv("ANTHROPIC_LIGHT_MODEL"),
                max_tokens=500,
                system=system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": f"Predict the timezone for: {context}"
                    }
                ],
                tools=[
                    {
                        "name": "timezone_prediction",
                        "description": "Predict timezone with validation",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "timezone": {
                                    "type": "string",
                                    "description": "The predicted timezone (must be from valid list or Etc/GMT pattern)"
                                },
                                "confidence": {
                                    "type": "string",
                                    "enum": ["high", "medium", "low"],
                                    "description": "Confidence level of prediction"
                                },
                                "reasoning": {
                                    "type": "string",
                                    "description": "Explanation for the timezone choice"
                                }
                            },
                            "required": ["timezone", "confidence", "reasoning"]
                        }
                    }
                ]
            )
            
            # Extract the tool use response
            tool_result = None
            for content_block in response.content:
                if content_block.type == "tool_use":
                    tool_result = content_block.input
                    break
            
            if not tool_result:
                return {
                    "timezone": None,
                    "confidence": "low",
                    "reasoning": "Failed to extract prediction from model response",
                    "is_valid": False,
                    "attempt": attempt,
                    "success": False,
                    "error_message": "No tool use output received"
                }
            
            # Validate the predicted timezone
            predicted_timezone = tool_result.get("timezone", "").strip()
            is_valid = is_valid_timezone(predicted_timezone)
            
            result = {
                "timezone": predicted_timezone,
                "confidence": tool_result.get("confidence", "low"),
                "reasoning": tool_result.get("reasoning", ""),
                "is_valid": is_valid,
                "attempt": attempt,
                "success": is_valid,
                "error_message": None if is_valid else f"Timezone '{predicted_timezone}' not in valid list"
            }
            
            # If valid on first attempt, return immediately
            if is_valid and attempt == 1:
                result["success"] = True
                return result
            
            # If valid, return it
            if is_valid:
                return result
            
            # If invalid, prepare for retry with fallback context
            if attempt < max_retries:
                if previous_predictions is None:
                    previous_predictions = []
                previous_predictions.append(predicted_timezone)
                continue
            else:
                # Max retries reached, return last failed attempt
                return result
                
        except anthropic.APIError as e:
            if attempt == max_retries:
                return {
                    "timezone": None,
                    "confidence": "low",
                    "reasoning": "API Error",
                    "is_valid": False,
                    "attempt": attempt,
                    "success": False,
                    "error_message": str(e)
                }
    
    return {
        "timezone": None,
        "confidence": "low",
        "reasoning": "Max retries exceeded",
        "is_valid": False,
        "attempt": max_retries,
        "success": False,
        "error_message": "Failed to predict valid timezone after maximum retries"
    }


print(predict_timezone("Austria")['timezone'])