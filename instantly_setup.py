import csv
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from instantly.campaigns import list_campaigns,create_campaign
from instantly.leads import upload_series_lead_to_campaign
from instantly.data_inputs import external_schema_converter, data_read
from instantly.grouping import existing_country_campaigns
from instantly.email_generation import  email_chain_generation, person_data_explorer, _build_ollama_client
from instantly.helper import restructure_response
import pandas as pd
from groq import Groq

load_dotenv()

client_ollama = _build_ollama_client()

client_groq = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

PROGRESS_LOG_FIELDS = [
    "timestamp_utc",
    "country",
    "campaign_id",
    "record_index",
    "record_key",
    "email",
    "person_linkedin",
    "full_name",
    "status",
    "error",
]

DEFAULT_PROGRESS_LOG_FILE = "instantly/instantly_setup_progress.csv"


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _build_record_key(record: pd.Series) -> str:
    email = _normalize_text(record.get("email", "")).lower()
    if email:
        return f"email:{email}"

    person_linkedin = _normalize_text(record.get("person_linkedin", "")).lower()
    if person_linkedin:
        return f"linkedin:{person_linkedin}"

    first_name = _normalize_text(record.get("first_name", "")).lower()
    last_name = _normalize_text(record.get("last_name", "")).lower()
    company_name = _normalize_text(record.get("company_name", "")).lower()
    return f"name_company:{first_name} {last_name}|{company_name}".strip()


def _load_processed_record_keys(progress_log_file: str) -> set[str]:
    processed: set[str] = set()
    if not os.path.exists(progress_log_file):
        return processed

    with open(progress_log_file, "r", newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if row.get("status") != "success":
                continue
            record_key = _normalize_text(row.get("record_key", ""))
            if record_key:
                processed.add(record_key)

    return processed


def _append_progress_row(progress_log_file: str, row: dict):
    directory = os.path.dirname(progress_log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)

    file_exists = os.path.exists(progress_log_file)

    with open(progress_log_file, "a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=PROGRESS_LOG_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in PROGRESS_LOG_FIELDS})


def _generate_email_chain_with_retries(record: pd.Series, max_attempts: int):
    max_attempts = max(1, int(max_attempts))
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            context = person_data_explorer(client_groq, record)
            return email_chain_generation(
                client=client_ollama,
                df=record,
                person_context=context,
                use_prefix_context_cache=True,
                max_generation_retries=max_attempts,
            )
        except Exception as exc:  # pragma: no cover - runtime/API-dependent branch
            last_error = exc
            if attempt < max_attempts:
                print(
                    f"[WARN] Email chain attempt {attempt}/{max_attempts} failed for "
                    f"{record.get('email', 'unknown email')}: {exc}. Retrying..."
                )
                time.sleep(1)

    raise RuntimeError(
        f"Email chain generation failed after {max_attempts} attempts: {last_error}"
    ) from last_error

def main(max_retries: int = 3):
    if client_ollama is None:
        raise RuntimeError(
            "Ollama client is not configured. Install ollama and set OLLAMA_HOST/OLLAMA_API_KEY as needed."
        )

    if not os.getenv("GROQ_API_KEY"):
        raise RuntimeError("GROQ_API_KEY is required for person context generation.")

    df = external_schema_converter(
        data_read(
            file_url="US NY - Financial Services - 10 mn to 500 mn - Copy.csv",
            sheet_name="in",
        )
    )

    campaigns_response = list_campaigns(
        api_key=os.getenv("INSTANTLY_API_KEY"),
        base_url=os.getenv("INSTANTLY_BASE_URL"),
    )

    non_matching_country_groups = existing_country_campaigns(campaigns_response, df)
    progress_log_file = os.getenv("INSTANTLY_SETUP_PROGRESS_FILE", DEFAULT_PROGRESS_LOG_FILE)
    processed_record_keys = _load_processed_record_keys(progress_log_file)

    print(
        f"Loaded {len(processed_record_keys)} previously successful records from {progress_log_file}"
    )

    final_series = pd.Series(dtype=object)
    total_uploaded_records = 0
    total_upload_seconds = 0.0
    total_failed_records = 0
    
    for country in non_matching_country_groups:
        for key , val in country.items():
            campaign_response = create_campaign(
                country_name=key,
                max_timezone_attempts=max_retries,
            )
            if campaign_response.get("error"):
                print(f"Campaign creation failed for {key}: {campaign_response['error']}")
                continue

            campaign_id = campaign_response.get("id")
            if not campaign_id:
                print(f"Campaign id missing for {key}: {campaign_response}")
                continue

            print(campaign_id)
            for i,record in enumerate(val):
                if i==1:
                    break
                record=pd.Series(record)
                record_key = _build_record_key(record)

                if record_key in processed_record_keys:
                    print(
                        f"[SKIP] Record {i} already processed: "
                        f"{record.get('email', record.get('person_linkedin', record_key))}"
                    )
                    continue

                try:
                    result = _generate_email_chain_with_retries(record, max_attempts=max_retries)

                    result = restructure_response(result)
                    final_series = pd.concat([record, result])
                    print(final_series)

                    upload_start = time.perf_counter()
                    upload_response = upload_series_lead_to_campaign(
                        lead_series=final_series,
                        campaign_id=campaign_id,
                        api_key=os.getenv("INSTANTLY_API_KEY"),
                        base_url=os.getenv("INSTANTLY_BASE_URL"),
                        skip_if_in_workspace=True,
                        verify_leads_on_import=False,
                    )

                    if upload_response.get("error"):
                        raise RuntimeError(upload_response.get("error"))

                    record_upload_seconds = time.perf_counter() - upload_start
                    total_uploaded_records += 1
                    total_upload_seconds += record_upload_seconds
                    processed_record_keys.add(record_key)

                    _append_progress_row(
                        progress_log_file,
                        {
                            "timestamp_utc": datetime.utcnow().isoformat(),
                            "country": key,
                            "campaign_id": campaign_id,
                            "record_index": i,
                            "record_key": record_key,
                            "email": _normalize_text(record.get("email", "")),
                            "person_linkedin": _normalize_text(record.get("person_linkedin", "")),
                            "full_name": (
                                f"{_normalize_text(record.get('first_name', ''))} "
                                f"{_normalize_text(record.get('last_name', ''))}"
                            ).strip(),
                            "status": "success",
                            "error": "",
                        },
                    )

                    print(
                        f"Record : {i} processed and uploaded to campaign {campaign_id} "
                        f"in {record_upload_seconds:.2f} seconds"
                    )
                except Exception as exc:
                    total_failed_records += 1
                    _append_progress_row(
                        progress_log_file,
                        {
                            "timestamp_utc": datetime.utcnow().isoformat(),
                            "country": key,
                            "campaign_id": campaign_id,
                            "record_index": i,
                            "record_key": record_key,
                            "email": _normalize_text(record.get("email", "")),
                            "person_linkedin": _normalize_text(record.get("person_linkedin", "")),
                            "full_name": (
                                f"{_normalize_text(record.get('first_name', ''))} "
                                f"{_normalize_text(record.get('last_name', ''))}"
                            ).strip(),
                            "status": "failed",
                            "error": str(exc),
                        },
                    )
                    print(
                        f"[FAIL] Skipping record {i} ({record.get('email', 'unknown email')}) "
                        f"after {max_retries} attempts: {exc}"
                    )
                    continue

    if total_uploaded_records:
        print(
            f"Total upload time for all records: {total_upload_seconds:.2f} seconds "
            f"for {total_uploaded_records} records"
        )
        print(
            f"Average upload time per record: "
            f"{(total_upload_seconds / total_uploaded_records):.2f} seconds"
        )
    else:
        print("No records were uploaded.")

    print(f"Total failed records in this run: {total_failed_records}")
    print(f"Progress log file: {progress_log_file}")

if __name__ == "__main__":
    main()