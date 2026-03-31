import numpy as np
import pandas as pd


def convert_to_serializable(obj):
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj) if not np.isnan(obj) else None
    if isinstance(obj, np.bool_):
        return bool(obj)
    if pd.isna(obj):
        return None
    return obj


def clean_nones(obj):
    if isinstance(obj, dict):
        cleaned = {}
        for key, value in obj.items():
            if isinstance(value, list):
                filtered_list = [
                    clean_nones(item)
                    for item in value
                    if item is not None and not (isinstance(item, float) and np.isnan(item))
                ]
                if filtered_list:
                    if "employee" in key.lower() and "range" in key.lower():
                        filtered_list = [
                            item.replace("-", ",") if isinstance(item, str) else item
                            for item in filtered_list
                        ]
                    cleaned[key] = filtered_list
            elif value is not None and value != "" and not (isinstance(value, float) and np.isnan(value)):
                cleaned[key] = clean_nones(value)
        return cleaned

    if isinstance(obj, list):
        return [clean_nones(item) for item in obj if item is not None]

    return obj


def build_filters_from_config_df(df: pd.DataFrame) -> dict:
    filters = {}

    for col in df.columns:
        if col.endswith("[]"):
            filters[col] = df[col].tolist()
            continue

        if (df[col] == "").all() or df[col].isnull().all():
            continue

        if col == "organization_industry_tags":
            filters[col] = [df[col][0]] if df[col][0] else []
        else:
            filters[col] = df[col][0]

    filters = convert_to_serializable(filters)
    filters = clean_nones(filters)
    return filters
