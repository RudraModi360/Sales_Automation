import pandas as pd

from apollo.summary import MASTER_SUMMARY_FIELDS
from apollo.summary import transform_overview_to_summary as _transform_overview_to_summary


mapping_dict = {
    "First Name": "first_name",
    "Last Name": "last_name",
    "Title": "job_title",
    "Company Name": "company_name",
    "Email": "email",
    "Seniority": "seniority",
    "Sub Departments": "subdepartment",
    "# Employees": "company_estimated_employees",
    "Industry": "company_industry",
    "Person Linkedin Url": "person_linkedin",
    "Website": "company_website",
    "Company Linkedin Url": "company_linkedin",
    "Twitter Url": "person_twitter",
    "City": "company_city",
    "State": "company_state",
    "Country": "company_country",
    "Company Address": "company_address",
    "Annual Revenue": "company_revenue",
    "Apollo Contact Id": "person.id",
    "Apollo Account Id": "person.organization.id",
}


SUMMARY_TO_OVERVIEW_MAPPING = {
    "full_name": "person.name",
    "first_name": "person.first_name",
    "last_name": "person.last_name",
    "email": "person.email",
    "job_title": "person.title",
    "headline": "person.headline",
    "seniority": "person.seniority",
    "function": "person.functions",
    "subdepartment": "person.subdepartments",
    "person_linkedin": "person.linkedin_url",
    "person_twitter": "person.twitter_url",
    "person_location": "person.formatted_address",
    "timezone": "person.time_zone",
    "company_name": "person.organization.name",
    "company_description": "person.organization.short_description",
    "company_website": "person.organization.website_url",
    "company_domain": "person.organization.primary_domain",
    "company_linkedin": "person.organization.linkedin_url",
    "company_twitter": "person.organization.twitter_url",
    "company_facebook": "person.organization.facebook_url",
    "company_phone": "person.organization.primary_phone.number",
    "company_industry": "person.organization.industry",
    "company_industries": "person.organization.industries",
    "company_secondary_industries": "person.organization.secondary_industries",
    "company_estimated_employees": "person.organization.estimated_num_employees",
    "company_revenue": "person.organization.annual_revenue_printed",
    "company_founded_year": "person.organization.founded_year",
    "company_languages": "person.organization.languages",
    "company_address": "person.organization.raw_address",
    "company_city": "person.organization.city",
    "company_state": "person.organization.state",
    "company_country": "person.organization.country",
}


def _normalize_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    text = str(value).strip()
    return text if text else None


def align_to_master_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=MASTER_SUMMARY_FIELDS)

    aligned_df = df.reindex(columns=MASTER_SUMMARY_FIELDS)
    return aligned_df.fillna("")


def external_to_overview_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        raise ValueError("Input DataFrame cannot be None")

    if df.empty:
        return pd.DataFrame(columns=list(SUMMARY_TO_OVERVIEW_MAPPING.values()))

    summary_df = pd.DataFrame("", index=df.index, columns=MASTER_SUMMARY_FIELDS)

    for external_column, summary_column in mapping_dict.items():
        if external_column in df.columns and summary_column in summary_df.columns:
            summary_df[summary_column] = df[external_column].apply(_normalize_value).fillna("")

    # Build full_name when first_name/last_name are present but full_name is not.
    if "full_name" in summary_df.columns:
        missing_full_name = summary_df["full_name"].eq("")
        first_names = summary_df["first_name"].fillna("").astype(str)
        last_names = summary_df["last_name"].fillna("").astype(str)
        combined_names = (first_names + " " + last_names).str.strip()
        summary_df.loc[missing_full_name, "full_name"] = combined_names[missing_full_name]

    overview_df = pd.DataFrame(index=df.index)
    for summary_column, overview_column in SUMMARY_TO_OVERVIEW_MAPPING.items():
        values = summary_df[summary_column].replace("", pd.NA)
        overview_df[overview_column] = values

    return overview_df


def transform_overview_to_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary_df = _transform_overview_to_summary(df)
    return align_to_master_schema(summary_df)
