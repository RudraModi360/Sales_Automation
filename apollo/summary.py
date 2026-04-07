import ast

import pandas as pd


MASTER_SUMMARY_FIELDS = [
    "full_name",
    "first_name",
    "last_name",
    "email",
    "job_title",
    "headline",
    "seniority",
    "function",
    "subdepartment",
    "person_linkedin",
    "person_twitter",
    "person_location",
    "timezone",
    "company_name",
    "company_description",
    "company_website",
    "company_domain",
    "company_linkedin",
    "company_twitter",
    "company_facebook",
    "company_phone",
    "company_industry",
    "company_industries",
    "company_secondary_industries",
    "company_estimated_employees",
    "company_revenue",
    "company_founded_year",
    "company_languages",
    "company_address",
    "company_city",
    "company_state",
    "company_country",
    "source",
]


def normalize_value(value):
    if value is None:
        return None

    # Some Apollo fields come back as arrays/lists; normalize these first to avoid
    # ambiguous truth-value errors from pd.isna(array_like).
    if isinstance(value, (list, tuple, set, pd.Series)):
        clean_items = []
        for item in value:
            if item is None:
                continue
            try:
                if pd.isna(item):
                    continue
            except (TypeError, ValueError):
                pass

            text_item = str(item).strip()
            if text_item:
                clean_items.append(text_item)

        return " | ".join(clean_items) if clean_items else None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    text = str(value).strip()
    if not text:
        return None

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                clean_items = [str(item).strip() for item in parsed if str(item).strip()]
                return " | ".join(clean_items) if clean_items else None
        except (ValueError, SyntaxError):
            return text

    return text


def pick_first_available(row, columns):
    for col in columns:
        if col in row.index:
            value = normalize_value(row[col])
            if value is not None:
                return value
    return None


def build_full_name(row):
    full_name = pick_first_available(row, ["person.name"])
    if full_name:
        return full_name

    first_name = pick_first_available(row, ["person.first_name"]) or ""
    last_name = pick_first_available(row, ["person.last_name"]) or ""
    combined = f"{first_name} {last_name}".strip()
    return combined if combined else None


def build_person_location(row):
    explicit = pick_first_available(row, ["person.formatted_address"])
    if explicit:
        return explicit

    city = pick_first_available(row, ["person.city"]) or ""
    state = pick_first_available(row, ["person.state"]) or ""
    country = pick_first_available(row, ["person.country"]) or ""
    parts = [part for part in [city, state, country] if part]
    return ", ".join(parts) if parts else None


def transform_overview_to_summary(df):
    records = []

    for _, row in df.iterrows():
        record = {
            "full_name": build_full_name(row),
            "first_name": pick_first_available(row, ["person.first_name"]),
            "last_name": pick_first_available(row, ["person.last_name"]),
            "email": pick_first_available(row, ["person.email"]),
            "job_title": pick_first_available(row, ["person.title"]),
            "headline": pick_first_available(row, ["person.headline"]),
            "seniority": pick_first_available(row, ["person.seniority"]),
            "function": pick_first_available(row, ["person.functions"]),
            "subdepartment": pick_first_available(row, ["person.subdepartments"]),
            "person_linkedin": pick_first_available(row, ["person.linkedin_url"]),
            "person_twitter": pick_first_available(row, ["person.twitter_url"]),
            "person_location": build_person_location(row),
            "timezone": pick_first_available(row, ["person.time_zone"]),
            "company_name": pick_first_available(row, ["person.organization.name"]),
            "company_description": pick_first_available(row, ["person.organization.short_description"]),
            "company_website": pick_first_available(row, ["person.organization.website_url"]),
            "company_domain": pick_first_available(row, ["person.organization.primary_domain"]),
            "company_linkedin": pick_first_available(row, ["person.organization.linkedin_url"]),
            "company_twitter": pick_first_available(row, ["person.organization.twitter_url"]),
            "company_facebook": pick_first_available(row, ["person.organization.facebook_url"]),
            "company_phone": pick_first_available(
                row,
                [
                    "person.organization.primary_phone.number",
                    "person.organization.phone",
                    "person.organization.primary_phone.sanitized_number",
                    "person.organization.sanitized_phone",
                ],
            ),
            "company_industry": pick_first_available(row, ["person.organization.industry"]),
            "company_industries": pick_first_available(row, ["person.organization.industries"]),
            "company_secondary_industries": pick_first_available(row, ["person.organization.secondary_industries"]),
            "company_estimated_employees": pick_first_available(row, ["person.organization.estimated_num_employees"]),
            "company_revenue": pick_first_available(
                row,
                [
                    "person.organization.organization_revenue_printed",
                    "person.organization.annual_revenue_printed",
                    "person.organization.organization_revenue",
                ],
            ),
            "company_founded_year": pick_first_available(row, ["person.organization.founded_year"]),
            "company_languages": pick_first_available(row, ["person.organization.languages"]),
            "company_address": pick_first_available(
                row,
                [
                    "person.organization.raw_address",
                    "person.organization.street_address",
                ],
            ),
            "company_city": pick_first_available(row, ["person.organization.city"]),
            "company_state": pick_first_available(row, ["person.organization.state"]),
            "company_country": pick_first_available(row, ["person.organization.country"]),
            "source": "apollo",
        }

        records.append(record)

    out_df = pd.DataFrame(records)
    out_df = out_df.reindex(columns=MASTER_SUMMARY_FIELDS)

    dedupe_keys = [key for key in ["email", "full_name", "company_name"] if key in out_df.columns]
    if dedupe_keys:
        out_df = out_df.drop_duplicates(subset=dedupe_keys, keep="first")

    if "email" in out_df.columns:
        out_df = out_df.sort_values(by=["email"], na_position="last")

    return out_df.fillna("").reset_index(drop=True)