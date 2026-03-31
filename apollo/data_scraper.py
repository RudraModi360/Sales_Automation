import pandas as pd
import numpy as np
import os
import json
import requests
from dotenv import load_dotenv

try:
    from utils.read_data import *
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    # Allow direct script execution from apollo/ by adding project root.
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from utils.read_data import *


load_dotenv()
# SharePoint Credentials
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRETS']
tenant_id = os.environ['TENANT_ID']
api_key=os.getenv("Apollo_API_KEY")

# File URL
file_url = "https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/Apollo_config.xlsx"

# Read config from SharePoint
df = get_sharepoint_file(file_url, client_id, client_secret, tenant_id, sheet_name="Config")

print(df.columns)
filters={}

for col in df.columns:
    if col.endswith("[]"):
        filters[col] = df[col].tolist()
    else:
        if (df[col] == "").all() or df[col].isnull().all():
            continue
        else:
            # For organization_industry_tags, always store as list
            if col == "organization_industry_tags":
                filters[col] = [df[col][0]] if df[col][0] else []
            else:
                filters[col] = df[col][0]

# Convert numpy types to native Python types and handle NaN values
def convert_to_serializable(obj):
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif pd.isna(obj):
        return None
    return obj

filters_clean = convert_to_serializable(filters)


# Remove None values and empty lists from dict, and convert employee ranges
def clean_nones(obj):
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if isinstance(v, list):
                # Filter out None values from lists
                filtered_list = [clean_nones(item) for item in v if item is not None and not (isinstance(item, float) and np.isnan(item))]
                # Only keep the key if list is not empty after filtering
                if filtered_list:
                    # Convert employee ranges from "51-100" to "51,100"
                    if "employee" in k.lower() and "range" in k.lower():
                        filtered_list = [item.replace("-", ",") if isinstance(item, str) else item for item in filtered_list]
                    cleaned[k] = filtered_list
            elif v is not None and v != "" and not (isinstance(v, float) and np.isnan(v)):
                cleaned[k] = clean_nones(v)
        return cleaned
    elif isinstance(obj, list):
        return [clean_nones(item) for item in obj if item is not None]
    return obj

filters_clean = clean_nones(filters_clean)

# print("filters from Excel (cleaned) : ",filters_clean)
# print(json.dumps(filters_clean, indent=2))


def search_apollo_people(
    filters: dict,
    api_key: str,
    batch_interval: int = 2000,
    batch_size: int = 5,
    max_pages: int = 2,
    timeout: int = 30
) -> dict:
    """
    Search for people in Apollo.io using mixed_people API endpoint.
    
    Args:
        filters (dict): Filter parameters for the search
        api_key (str): Apollo API key
        batch_size (int): Number of records per batch (default: 5)
        batch_interval (int): Interval between batches in milliseconds (default: 2000)
        max_pages (int): Maximum number of pages to fetch (default: 20)
        timeout (int): Request timeout in seconds (default: 30)
    
    Returns:
        dict: Combined results from all pages
    """
    import time
    
    url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    
    headers = {
        "Cache-Control": "no-cache",
        "accept": "application/json",
        "x-api-key": api_key
    }
    
    all_results = {
        "people": [],
        "meta": {},
        "page_count": 0
    }
    
    try:
        for page in range(1, max_pages + 1):
            # Add pagination parameter
            payload = filters.copy()
            payload["page"] = page
            
            print(f"Fetching page {page}...")
            
            response = requests.post(
                url,
                params=payload,
                headers=headers,
                timeout=timeout
            )
            
            
            response.raise_for_status()
            data = response.json()
            
            if "people" in data:
                all_results["people"].extend(data["people"])
            
            if "meta" in data:
                all_results["meta"] = data["meta"]
                all_results["page_count"] = page
                
                # Check if we've reached the last page
                if data["meta"].get("pagination", {}).get("total_pages", 1) == page:
                    print(f"Reached last page: {page}")
                    break
            
            # Wait between batches (convert milliseconds to seconds)
            if page < max_pages:
                time.sleep(batch_interval / 1000)
        
        all_results['total_records']=data['total_entries']
        return all_results
    
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return {"error": str(e), "people": [], "meta": {}}


def build_overview_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Build a high-level view by removing noisy low-value fields."""
    clutter_tokens = [
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

    overview_cols = [
        col
        for col in raw_df.columns
        if not any(token in col.lower() for token in clutter_tokens)
    ]
    overview_df = raw_df[overview_cols].copy()

    preferred_order = [
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
    final_order = [col for col in preferred_order if col in overview_df.columns]
    final_order += [col for col in overview_df.columns if col not in final_order]

    return overview_df[final_order]


def save_excel_outputs(raw_rows, raw_output_path, overview_output_path):
    raw_df = pd.DataFrame(raw_rows)
    if raw_df.empty:
        print("No enrichment rows to save.")
        return

    if "person.id" in raw_df.columns:
        raw_df = raw_df.drop_duplicates(subset=["person.id"], keep="last")

    overview_df = build_overview_df(raw_df)

    raw_df.to_excel(raw_output_path, index=False)
    overview_df.to_excel(overview_output_path, index=False)

    print(f"Saved raw Excel: {raw_output_path} ({len(raw_df)} rows)")
    print(f"Saved overview Excel: {overview_output_path} ({len(overview_df)} rows)")

api_key = os.getenv("Apollo_API_KEY") or os.getenv("APOLLO_API_KEY")
if not api_key:
    raise ValueError("Apollo_API_KEY/APOLLO_API_KEY is not set in environment variables.")

results = search_apollo_people(filters_clean, api_key)

print("Total people found:", results['total_records'])


def get_people_enrich(
    api_key,
    results,
    timeout,
    raw_excel_path="apollo_people_match_raw.xlsx",
    overview_excel_path="apollo_people_match_overview.xlsx",
):
    url = "https://api.apollo.io/api/v1/people/match"

    headers = {
        "Cache-Control": "no-cache",
        "accept": "application/json",
        "x-api-key": api_key
    }

    success_count = 0
    failed_count = 0
    raw_rows = []

    for i,people in enumerate(results.get('people', [])):
        if i==1:
            break
        required = {
            "first_name": people.get('first_name'),
            "organization_name": (people.get('organization') or {}).get('name'),
            "id": people.get('id'),
            "reveal_personal_emails": True,
            "reveal_phone_number": True
        }

        try:
            response = requests.post(
                url,
                params=required,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            data = response.json()

            flat = pd.json_normalize(data, sep='.')
            if not flat.empty:
                raw_rows.append(flat.iloc[0].to_dict())
                success_count += 1
                print(f"Collected {success_count} enrichment rows")
            else:
                failed_count += 1
                print(f"Empty response for id={required['id']}")

        except requests.exceptions.RequestException as e:
            failed_count += 1
            print(f"Failed for id={required['id']}: {e}")

    save_excel_outputs(raw_rows, raw_excel_path, overview_excel_path)

    print(f"Enrichment complete. Success={success_count}, Failed={failed_count}")
        
print("Started To Enrich People ....")
get_people_enrich(api_key=api_key,results=results,timeout=300)
