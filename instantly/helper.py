import pandas as pd
import json
import ast

EMAIL_PREFIXES = ["main_email", "followup_1", "followup_2", "followup_3", "followup_4"]
COMPONENTS = ["introduction", "value_proposition", "call_to_action"]

# Supports both old and current type labels
TYPE_TO_PREFIX = {
    "main_email": "main_email",
    "followup_1": "followup_1",
    "followup_2": "followup_2",
    "followup_3": "followup_3",
    "followup_4": "followup_4",
    "followup_email_1": "followup_1",
    "followup_email_2": "followup_2",
    "followup_email_3": "followup_3",
    "followup_email_4": "followup_4",
}

def restructure_response(records) -> pd.Series:
    """
    Flatten a list of email generation results into a single pd.Series with 15 fields.
    Column names match exactly the template variables in email_template.py.
    
    Args:
        records: List of dicts from email_chain_generation(), or single dict for backward compatibility
        
    Returns:
        pd.Series with 15 fields: main_email_{component}, followup_1..4_{component}
        where component = introduction, value_proposition, call_to_action
        
    Example:
        results = email_chain_generation(client, df, context)
        row = restructure_response(results)
    """
    # Backward-compatible: allow single dict input
    if isinstance(records, dict):
        records = [records]

    # Pre-create all 15 fields with empty defaults (3 components × 5 emails)
    flat = {
        f"{prefix}_{component}": ""
        for prefix in EMAIL_PREFIXES
        for component in COMPONENTS
    }

    for record in records:
        prefix = TYPE_TO_PREFIX.get(record.get("type"))
        if not prefix:
            continue

        structured = record.get("structured_output") or {}
        for component in COMPONENTS:
            flat[f"{prefix}_{component}"] = structured.get(component, "")

    return pd.Series(flat)

if __name__ == "__main__":
    with open("results.txt", 'r') as f:
        content = ast.literal_eval(f.read())
    
    result = restructure_response(content)
    print(result.keys())

