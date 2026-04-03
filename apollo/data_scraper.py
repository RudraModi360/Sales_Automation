from __future__ import annotations

def _ensure_project_root_on_path() -> None:
    """Allow direct script execution from apollo/ while keeping imports explicit."""
    import sys
    from pathlib import Path

    root = str(Path(__file__).resolve().parents[1])
    if root not in sys.path:
        sys.path.append(root)


_ensure_project_root_on_path()

from apollo.pipeline.apollo_client import collect_enriched_rows, search_people
from apollo.pipeline.config import load_settings
from apollo.pipeline.filters import build_filters_from_config_df
from apollo.pipeline.frames import build_overview_df, build_raw_df
from utils.read_data import get_sharepoint_file, upload_excel_sheets_to_sharepoint
from utils.summary_transform import transform_overview_to_summary


def save_pipeline_workbook(raw_rows: list[dict], workbook_output_url: str, settings) -> None:
    new_raw_df = build_raw_df(raw_rows)
    if new_raw_df.empty:
        print("No enrichment rows to save.")
        return

    existing_raw_df = None
    try:
        existing_sheets = get_sharepoint_file(
            workbook_output_url,
            settings.client_id,
            settings.client_secret,
            settings.tenant_id,
            sheet_name=None,
        )
        if isinstance(existing_sheets, dict) and "raw" in existing_sheets:
            existing_raw_df = existing_sheets["raw"]
    except Exception as exc:
        # If workbook is missing or inaccessible, continue with current run rows only.
        print(f"Could not read existing workbook raw sheet; continuing with new rows only. Reason: {exc}")

    if existing_raw_df is not None and not existing_raw_df.empty:
        combined_raw_df = build_raw_df([*existing_raw_df.to_dict(orient="records"), *new_raw_df.to_dict(orient="records")])
        print(
            "Appending new rows into existing raw sheet: "
            f"existing={len(existing_raw_df)}, new={len(new_raw_df)}, combined={len(combined_raw_df)}"
        )
    else:
        combined_raw_df = new_raw_df
        print(f"No existing raw sheet found. Starting raw sheet with {len(combined_raw_df)} rows")

    print("Phase 1 complete: built raw dataset")
    overview_df = build_overview_df(combined_raw_df)
    print("Phase 2 complete: built overview dataset")
    summary_df = transform_overview_to_summary(overview_df)
    print("Phase 3 complete: built summary dataset")

    upload = upload_excel_sheets_to_sharepoint(
        workbook_output_url,
        {
            "raw": combined_raw_df,
            "overview": overview_df,
            "summary": summary_df,
        },
        settings.client_id,
        settings.client_secret,
        settings.tenant_id,
    )

    workbook_web_url = upload.get("webUrl", "<webUrl unavailable>")
    workbook_resolved = upload.get("_resolved_drive_path", "<unknown>")
    workbook_sheets = upload.get("_sheet_names", [])
    upload_mode = upload.get("_upload_mode", "unknown")
    upload_status_code = upload.get("_upload_status_code", "unknown")
    created_new_file = upload.get("_created_new_file", "unknown")

    print(f"Uploaded workbook to SharePoint: {workbook_output_url}")
    print(
        "Updated sheets: "
        f"raw ({len(combined_raw_df)} rows), "
        f"overview ({len(overview_df)} rows), "
        f"summary ({len(summary_df)} rows)"
    )
    print(f"Workbook actual SharePoint URL: {workbook_web_url}")
    print(f"Workbook resolved drive path: {workbook_resolved}")
    print(f"Workbook upload mode: {upload_mode} (HTTP {upload_status_code})")
    print(f"Workbook created as new file: {created_new_file}")
    print(f"Workbook sheets currently present: {workbook_sheets}")


def main() -> None:
    """Run end-to-end Apollo enrichment and publish workbook sheets."""
    settings = load_settings()

    config_df = get_sharepoint_file(
        settings.config_file_url,
        settings.client_id,
        settings.client_secret,
        settings.tenant_id,
        sheet_name="Config",
    )
    print("Loaded config columns:", list(config_df.columns))

    filters = build_filters_from_config_df(config_df)
    print("Filters : ", filters)
    print("__"*50)
    results = search_people(filters, settings.api_key)
    print("Total people found:", results.get("total_records", 0))

    print("Started To Enrich People ....")
    raw_rows, success_count, failed_count = collect_enriched_rows(
        settings.api_key,
        results,
        timeout=300,
        max_enrich_rows=settings.max_enrich_rows,
    )

    save_pipeline_workbook(raw_rows, settings.workbook_output_url, settings)
    print(f"Enrichment complete. Success={success_count}, Failed={failed_count}")


if __name__ == "__main__":
    main()
