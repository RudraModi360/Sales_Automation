import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class PipelineSettings:
    client_id: str
    client_secret: str
    tenant_id: str
    api_key: str
    config_file_url: str
    workbook_output_url: str
    max_enrich_rows: int | None


def _optional_positive_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def load_settings() -> PipelineSettings:
    load_dotenv()

    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRETS"]
    tenant_id = os.environ["TENANT_ID"]
    api_key = os.getenv("Apollo_API_KEY") or os.getenv("APOLLO_API_KEY")
    if not api_key:
        raise ValueError("Apollo_API_KEY/APOLLO_API_KEY is not set in environment variables.")

    config_file_url = os.getenv(
        "APOLLO_CONFIG_FILE_URL",
        "https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/Apollo_config.xlsx",
    )
    workbook_output_url = os.getenv(
        "APOLLO_WORKBOOK_OUTPUT_URL",
        "https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/apollo_people_pipeline.xlsx",
    )
    max_enrich_rows = _optional_positive_int(os.getenv("MAX_ENRICH_ROWS"))

    return PipelineSettings(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        api_key=api_key,
        config_file_url=config_file_url,
        workbook_output_url=workbook_output_url,
        max_enrich_rows=max_enrich_rows,
    )
