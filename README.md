# Apollo People Pipeline

Simple pipeline to fetch Apollo people, enrich them, and update a SharePoint workbook.

## Output Workbook Sheets
- raw: full enriched rows from Apollo
- overview: cleaned high-level fields
- summary: outreach-ready final fields

## Quick Start
1. Set required environment variables in .env:
- CLIENT_ID
- CLIENT_SECRETS
- TENANT_ID
- Apollo_API_KEY (or APOLLO_API_KEY)

2. Run full pipeline:
- python apollo/data_scraper.py

3. Refresh only summary from existing overview sheet:
- python cleaning.py

## Optional Environment Variables
- APOLLO_CONFIG_FILE_URL
- APOLLO_WORKBOOK_OUTPUT_URL
- MAX_ENRICH_ROWS (useful for small test runs)

## File Map
- apollo/data_scraper.py: main orchestration
- apollo/pipeline/config.py: settings/env loader
- apollo/pipeline/filters.py: config-to-filter conversion
- apollo/pipeline/apollo_client.py: Apollo API calls
- apollo/pipeline/frames.py: raw and overview builders
- utils/read_data.py: SharePoint read/write helpers
- utils/summary_transform.py: overview-to-summary transform
- cleaning.py: summary updater job

## Troubleshooting
- Missing env vars: script exits early with clear error.
- SharePoint file errors: check URL + tenant/app credentials.
- Too few records: check MAX_ENRICH_ROWS and Apollo filters.
