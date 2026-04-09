import csv
import importlib
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from instantly.campaigns import list_campaigns
from instantly.data_inputs import data_read, external_schema_converter
from instantly.grouping import existing_country_campaigns


load_dotenv()


REQUIRED_PERSON_FIELDS = [
    "first_name",
    "last_name",
    "job_title",
    "seniority",
    "company_name",
    "company_description",
    "company_website",
    "person_linkedin",
    "person_twitter",
    "company_linkedin",
    "company_industry",
    "email",
    "company_country",
]


RUN_LOG_FIELDS = [
    "timestamp_utc",
    "country",
    "record_index",
    "full_name",
    "email",
    "status",
    "error",
    "output_file",
]


class RecordRow(dict):
    """Dictionary wrapper compatible with prompt helpers expecting a .to_dict() method."""

    def to_dict(self) -> dict[str, Any]:
        return dict(self)


def _load_email_generation_bindings() -> dict[str, Any]:
    module = importlib.import_module("instantly.email_generation")

    required_symbols = [
        "client",
        "client_groq",
        "person_data_explorer",
        "email_chain_generation",
        "save_email_chain_results",
    ]
    missing = [name for name in required_symbols if not hasattr(module, name)]
    if missing:
        raise RuntimeError(
            "instantly.email_generation is missing required symbols: " + ", ".join(sorted(missing))
        )

    return {
        "default_ollama_client": getattr(module, "client"),
        "groq_client": getattr(module, "client_groq"),
        "person_data_explorer": getattr(module, "person_data_explorer"),
        "email_chain_generation": getattr(module, "email_chain_generation"),
        "save_email_chain_results": getattr(module, "save_email_chain_results"),
    }


def _build_ollama_client(default_client: Any):
    if default_client is not None:
        return default_client

    try:
        ollama_module = importlib.import_module("ollama")
    except ModuleNotFoundError as exc:
        raise RuntimeError("ollama package is not installed. Install it before running outreach integration.") from exc

    client_class = getattr(ollama_module, "Client", None)
    if client_class is None:
        raise RuntimeError("Unable to initialize Ollama client from ollama package.")

    host = (os.getenv("OLLAMA_HOST") or "").strip()
    api_key = (os.getenv("OLLAMA_API_KEY") or "").strip()

    if api_key:
        return client_class(
            host=host or "https://ollama.com",
            headers={"Authorization": f"Bearer {api_key}"},
        )

    if host:
        return client_class(host=host)

    return client_class()


def _normalize_value(value: Any) -> Any:
    if value is None:
        return ""

    if isinstance(value, float) and math.isnan(value):
        return ""

    return value


def _normalize_record(record: dict[str, Any]) -> RecordRow:
    normalized = {k: _normalize_value(v) for k, v in record.items()}
    for field in REQUIRED_PERSON_FIELDS:
        normalized.setdefault(field, "")
    return RecordRow(normalized)


def _append_run_log(log_rows: list[dict[str, Any]], run_log_file: str) -> str:
    if not log_rows:
        return run_log_file

    log_path = Path(run_log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    existing_logs: list[dict[str, Any]] = []
    if log_path.exists():
        with log_path.open("r", newline="", encoding="utf-8") as existing_file:
            reader = csv.DictReader(existing_file)
            existing_logs = list(reader)

    all_logs = existing_logs + log_rows

    with log_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=RUN_LOG_FIELDS)
        writer.writeheader()
        for row in all_logs:
            writer.writerow({field: row.get(field, "") for field in RUN_LOG_FIELDS})

    return str(log_path)


def run_non_matching_outreach(
    file_url: str | None = None,
    sheet_name: str = "summary",
    output_file: str = "instantly/email_chain_output.xlsx",
    run_log_file: str = "instantly/non_matching_outreach_run_log.csv",
    max_countries: int | None = None,
    max_records_per_country: int | None = None,
) -> dict[str, Any]:
    bindings = _load_email_generation_bindings()
    ollama_client = _build_ollama_client(bindings["default_ollama_client"])
    groq_client = bindings["groq_client"]

    if groq_client is None:
        raise RuntimeError("Groq client is not available in instantly.email_generation.")

    if not os.getenv("GROQ_API_KEY"):
        raise RuntimeError("GROQ_API_KEY is required for person context generation.")

    df = external_schema_converter(
        data_read(
            file_url=file_url,
            sheet_name=sheet_name,
        )
    )

    campaigns_response = list_campaigns(
        api_key=os.getenv("INSTANTLY_API_KEY"),
        base_url=os.getenv("INSTANTLY_BASE_URL"),
    )
    if campaigns_response.get("error"):
        raise RuntimeError(f"Failed to fetch campaigns: {campaigns_response['error']}")

    non_matching_country_groups = existing_country_campaigns(campaigns_response, df)
    if max_countries is not None:
        non_matching_country_groups = non_matching_country_groups[:max_countries]

    processed_count = 0
    failed_count = 0
    total_countries = 0
    log_rows: list[dict[str, Any]] = []

    for country_group in non_matching_country_groups:
        if not country_group:
            continue

        country_name = next(iter(country_group.keys()))
        country_records = country_group.get(country_name, [])
        if not isinstance(country_records, list):
            continue

        total_countries += 1
        print(f"Processing country: {country_name} | records: {len(country_records)}")

        per_country_processed = 0
        for record_index, record in enumerate(country_records, start=1):
            if max_records_per_country is not None and per_country_processed >= max_records_per_country:
                break

            if not isinstance(record, dict):
                continue

            current_timestamp = datetime.utcnow().isoformat()
            normalized_row = _normalize_record(record)
            identifier_email = str(normalized_row.get("email", ""))
            identifier_name = f"{normalized_row.get('first_name', '')} {normalized_row.get('last_name', '')}".strip()

            try:
                person_context = bindings["person_data_explorer"](groq_client, normalized_row)
                chain_results = bindings["email_chain_generation"](
                    client=ollama_client,
                    df=normalized_row,
                    person_context=person_context,
                )
                _, saved_file = bindings["save_email_chain_results"](chain_results, output_file=output_file)

                processed_count += 1
                per_country_processed += 1

                log_rows.append(
                    {
                        "timestamp_utc": current_timestamp,
                        "country": country_name,
                        "record_index": record_index,
                        "full_name": identifier_name,
                        "email": identifier_email,
                        "status": "success",
                        "error": "",
                        "output_file": saved_file,
                    }
                )
                print(
                    f"  [OK] {country_name} | record {record_index} | "
                    f"{identifier_email or identifier_name or 'unknown'}"
                )
            except Exception as exc:  # pragma: no cover - runtime/API-dependent branch
                failed_count += 1
                log_rows.append(
                    {
                        "timestamp_utc": current_timestamp,
                        "country": country_name,
                        "record_index": record_index,
                        "full_name": identifier_name,
                        "email": identifier_email,
                        "status": "failed",
                        "error": str(exc),
                        "output_file": output_file,
                    }
                )
                print(
                    f"  [FAIL] {country_name} | record {record_index} | "
                    f"{identifier_email or identifier_name or 'unknown'} | {exc}"
                )

    run_log_path = _append_run_log(log_rows, run_log_file)
    summary = {
        "countries_processed": total_countries,
        "records_processed": processed_count,
        "records_failed": failed_count,
        "output_file": output_file,
        "run_log_file": run_log_path,
    }

    print("\nOutreach Integration Summary")
    print("-" * 40)
    print(f"Countries processed: {summary['countries_processed']}")
    print(f"Records processed:   {summary['records_processed']}")
    print(f"Records failed:      {summary['records_failed']}")
    print(f"Email output file:   {summary['output_file']}")
    print(f"Run log file:        {summary['run_log_file']}")

    return summary


if __name__ == "__main__":
    run_non_matching_outreach()