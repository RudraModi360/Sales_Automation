import numpy as np
import pandas as pd


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

filters={}


def build_filters_from_config_df(df : pd.DataFrame) -> dict:
    filters = {}
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
    filters_clean = convert_to_serializable(filters)
    filters_clean = clean_nones(filters_clean)
    return filters_clean

# print("filters from Excel (cleaned) : ",filters_clean)
# print(json.dumps(filters_clean, indent=2))