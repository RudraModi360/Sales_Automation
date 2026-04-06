import pandas as pd


CLUTTER_TOKENS = [
    "keywords",
    "funding_events",
    "waterfall",
    "employment_history",
    "sic_codes",
    "naics_codes",
    "industry_tag_hash",
    "technology_names",
    "current_technologies",
    "suborganizations",
    "org_chart",
    "intent_strength",
    "show_intent",
]

PREFERRED_OVERVIEW_ORDER = [
    "person.id",
    "person.name",
    "person.first_name",
    "person.last_name",
    "person.email",
    "person.email_status",
    "person.title",
    "person.headline",
    "person.linkedin_url",
    "person.organization.id",
    "person.organization.name",
    "person.organization.short_description",
    "person.organization.website_url",
    "person.organization.primary_domain",
    "person.organization.primary_phone.number",
    "person.organization.phone",
    "person.organization.industry",
    "person.organization.estimated_num_employees",
    "person.organization.city",
    "person.organization.state",
    "person.organization.country",
]


def build_raw_df(raw_rows: list[dict]) -> pd.DataFrame:
    raw_df = pd.DataFrame(raw_rows)
    if raw_df.empty:
        return raw_df

    if "person.id" in raw_df.columns:
        raw_df = raw_df.drop_duplicates(subset=["person.id"], keep="last")

    return raw_df


def build_overview_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    overview_cols = [
        col
        for col in raw_df.columns
        if not any(token in col.lower() for token in CLUTTER_TOKENS)
    ]
    overview_df = raw_df[overview_cols].copy()

    final_order = [col for col in PREFERRED_OVERVIEW_ORDER if col in overview_df.columns]
    final_order += [col for col in overview_df.columns if col not in final_order]

    return overview_df[final_order]