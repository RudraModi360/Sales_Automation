import pandas as pd
import os
from .data_inputs import data_read
from .campaigns import extract_country_names
from dotenv import load_dotenv
from typing import Any

load_dotenv()

def group_by_country(df: pd.DataFrame) -> list[dict[str, list[dict[str, Any]]]]:
	if df is None:
		raise ValueError("Input DataFrame cannot be None")

	if df.empty:
		return []

	if "company_country" in df.columns:
		country_column = "company_country"
	elif "country" in df.columns:
		country_column = "country"
	else:
		raise ValueError("Country column not found. Expected 'company_country' or 'country'.")

	grouped_records: list[dict[str, list[dict[str, Any]]]] = []
	normalized_df = df.copy()
	normalized_df[country_column] = normalized_df[country_column].fillna("").astype(str).str.strip()

	for country_name, country_df in normalized_df.groupby(country_column, sort=True):
		key = country_name if country_name else "Unknown"
		grouped_records.append({key: country_df.to_dict(orient="records")})

	return grouped_records

def existing_country_campaigns(
	countries: list[dict[str, Any]] | dict[str, Any],
	df: pd.DataFrame,
) -> list[dict[str, list[dict[str, Any]]]]:
	"""
	Return grouped country records that do not yet have an existing campaign.

	Args:
		countries: Campaign response dict (with `items`) or `items` list directly
		df: Input DataFrame to group by country

	Returns:
		list[dict[str, list[dict[str, Any]]]]: Only non-matching grouped countries
	"""
	if df is None:
		raise ValueError("Input DataFrame cannot be None")

	if df.empty:
		return []

	grouped_countries = group_by_country(df)

	if isinstance(countries, dict):
		campaign_country_names = extract_country_names(countries)
	elif isinstance(countries, list):
		campaign_country_names = extract_country_names({"items": countries})
	else:
		raise ValueError("countries must be a campaign response dict or list of campaign dicts")

	existing_campaign_countries = {
		country_name.strip().casefold()
		for country_name in campaign_country_names
		if isinstance(country_name, str) and country_name.strip()
	}

	non_matching_grouped_countries: list[dict[str, list[dict[str, Any]]]] = []
	for country_group in grouped_countries:
		if not country_group:
			continue

		country_name = next(iter(country_group.keys()))
		if country_name.strip().casefold() not in existing_campaign_countries:
			non_matching_grouped_countries.append(country_group)

	return non_matching_grouped_countries