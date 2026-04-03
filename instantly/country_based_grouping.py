import os
from urllib.parse import unquote, urlparse

import pandas as pd
from dotenv import load_dotenv

try:
	from utils.read_data import get_sharepoint_file, normalize_sharepoint_file_url
except ModuleNotFoundError:
	import sys
	from pathlib import Path

	# Allow direct script execution from instantly/ by adding project root.
	sys.path.append(str(Path(__file__).resolve().parents[1]))
	from utils.read_data import get_sharepoint_file, normalize_sharepoint_file_url


load_dotenv()

CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRETS"]
TENANT_ID = os.environ["TENANT_ID"]


SUMMARY_SHEET_NAME = "summary"
DOC_SHAREPOINT_URL = os.getenv(
	"APOLLO_WORKBOOK_OUTPUT_URL",
	"https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/apollo_people_pipeline.xlsx",
)


def read_sharepoint_workbook_as_df(sharepoint_url: str) -> pd.DataFrame:
	parsed = urlparse(sharepoint_url)
	if not parsed.scheme or not parsed.netloc:
		raise ValueError("Invalid SharePoint URL provided.")

	_ = unquote(parsed.path)  # Keep minimal validation/normalization.
	sharepoint_url = normalize_sharepoint_file_url(sharepoint_url)

	return get_sharepoint_file(
		sharepoint_url,
		CLIENT_ID,
		CLIENT_SECRET,
		TENANT_ID,
		sheet_name=SUMMARY_SHEET_NAME,
	)


def get_country_grouped_output(sharepoint_url: str) -> list[dict]:
	df = read_sharepoint_workbook_as_df(sharepoint_url)

	if "company_country" not in df.columns:
		raise KeyError("'company_country' column not found in summary data.")

	group_df = df.copy()
	group_df["company_country"] = (
		group_df["company_country"]
		.fillna("Unknown")
		.astype(str)
		.str.strip()
		.replace("", "Unknown")
	)

	return [
		{country: group.reset_index(drop=True).to_dict(orient="records")}
		for country, group in group_df.groupby("company_country", sort=True)
	]


if __name__ == "__main__":
	country_grouped_list = get_country_grouped_output(DOC_SHAREPOINT_URL)
	# print(country_grouped_list[1])

	