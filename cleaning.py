import os
import pandas as pd
from dotenv import load_dotenv

from utils.read_data import get_sharepoint_file, upload_excel_sheets_to_sharepoint
from utils.summary_transform import transform_overview_to_summary

load_dotenv()

CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRETS"]
TENANT_ID = os.environ["TENANT_ID"]

WORKBOOK_FILE_URL = "https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/apollo_people_pipeline.xlsx"


def main():
	df = get_sharepoint_file(
		WORKBOOK_FILE_URL,
		CLIENT_ID,
		CLIENT_SECRET,
		TENANT_ID,
		sheet_name="overview",
	)
	cleaned = transform_overview_to_summary(df)
	upload_response = upload_excel_sheets_to_sharepoint(
		WORKBOOK_FILE_URL,
		{"summary": cleaned},
		CLIENT_ID,
		CLIENT_SECRET,
		TENANT_ID,
	)

	print(f"Input rows: {len(df)}")
	print(f"Input columns: {len(df.columns)}")
	print(f"Output rows: {len(cleaned)}")
	print(f"Output columns: {len(cleaned.columns)}")
	print(f"Updated workbook: {WORKBOOK_FILE_URL}")
	print(f"Actual SharePoint URL: {upload_response.get('webUrl', '<webUrl unavailable>')}")
	print(f"Resolved drive path: {upload_response.get('_resolved_drive_path', '<unknown>')}")
	print(f"Workbook sheets currently present: {upload_response.get('_sheet_names', [])}")
	print("Final columns:")
	for col in cleaned.columns:
		print(f"- {col}")


if __name__ == "__main__":
	main()