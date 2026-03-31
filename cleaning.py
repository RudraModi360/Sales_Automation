import ast
import pandas as pd

INPUT_FILE = "apollo_people_match_overview.xlsx"
OUTPUT_FILE = "apollo_people_summary.xlsx"


def normalize_value(value):
	if pd.isna(value):
		return None
	text = str(value).strip()
	if not text:
		return None

	# Convert stringified lists to pipe-separated readable text.
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


def transform(df):
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

	# Remove near-empty columns to keep the final dataset focused.
	keep_mask = out_df.notna().sum() > 0
	out_df = out_df.loc[:, keep_mask]

	# De-duplicate by best business key for outreach.
	dedupe_keys = [key for key in ["email", "full_name", "company_name"] if key in out_df.columns]
	if dedupe_keys:
		out_df = out_df.drop_duplicates(subset=dedupe_keys, keep="first")

	# Prioritize rows with available emails.
	if "email" in out_df.columns:
		out_df = out_df.sort_values(by=["email"], na_position="last")

	return out_df.reset_index(drop=True)


def main():
	df = pd.read_excel(INPUT_FILE)
	cleaned = transform(df)
	cleaned.to_excel(OUTPUT_FILE, index=False)

	print(f"Input rows: {len(df)}")
	print(f"Input columns: {len(df.columns)}")
	print(f"Output rows: {len(cleaned)}")
	print(f"Output columns: {len(cleaned.columns)}")
	print(f"Saved file: {OUTPUT_FILE}")
	print("Final columns:")
	for col in cleaned.columns:
		print(f"- {col}")


if __name__ == "__main__":
	main()