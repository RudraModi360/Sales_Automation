"""
Shared format variables for email body components.
"""

INTRODUCTION_VAR = "introduction"
VALUE_PROPOSITION_VAR = "value_proposition"
CALL_TO_ACTION_VAR = "call_to_action"


def build_email_body_from_parts(parts: dict) -> str:
    """
    Build a final email body from the 3 structured components.
    """
    introduction = str(parts.get(INTRODUCTION_VAR, "") or "").strip()
    value_proposition = str(parts.get(VALUE_PROPOSITION_VAR, "") or "").strip()
    call_to_action = str(parts.get(CALL_TO_ACTION_VAR, "") or "").strip()

    sections = [introduction, value_proposition, call_to_action]
    return "\n\n".join([section for section in sections if section])
