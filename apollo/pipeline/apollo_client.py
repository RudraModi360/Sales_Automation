import time
import pandas as pd
import requests


def search_people(
    filters: dict,
    api_key: str,
    batch_interval: int = 2000,
    max_pages: int = 2,
    timeout: int = 30,
) -> dict:
    url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    headers = {
        "Cache-Control": "no-cache",
        "accept": "application/json",
        "x-api-key": api_key,
    }

    all_results = {
        "people": [],
        "meta": {},
        "page_count": 0,
    }

    try:
        for page in range(1, max_pages + 1):
            payload = filters.copy()
            payload["page"] = page

            print(f"Fetching page {page}...")
            response = requests.post(
                url,
                params=payload,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()

            if "people" in data:
                all_results["people"].extend(data["people"])

            if "meta" in data:
                all_results["meta"] = data["meta"]
                all_results["page_count"] = page

                if data["meta"].get("pagination", {}).get("total_pages", 1) == page:
                    print(f"Reached last page: {page}")
                    break

            if page < max_pages:
                time.sleep(batch_interval / 1000)

        all_results["total_records"] = data.get("total_entries", 0)
        return all_results

    except requests.exceptions.RequestException as exc:
        print(f"Error during API request: {exc}")
        return {"error": str(exc), "people": [], "meta": {}, "total_records": 0}


def collect_enriched_rows(
    api_key: str,
    results: dict,
    timeout: int,
    max_enrich_rows: int | None = None,
) -> tuple[list[dict], int, int]:
    url = "https://api.apollo.io/api/v1/people/match"
    headers = {
        "Cache-Control": "no-cache",
        "accept": "application/json",
        "x-api-key": api_key,
    }

    success_count = 0
    failed_count = 0
    raw_rows: list[dict] = []

    for i, person in enumerate(results.get("people", [])):
        if max_enrich_rows is not None and i >= max_enrich_rows:
            print(f"Reached MAX_ENRICH_ROWS={max_enrich_rows}; stopping enrichment loop.")
            break

        required = {
            "first_name": person.get("first_name"),
            "organization_name": (person.get("organization") or {}).get("name"),
            "id": person.get("id"),
            "reveal_personal_emails": True,
            "reveal_phone_number": True,
        }

        try:
            response = requests.post(
                url,
                params=required,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()

            flat = pd.json_normalize(data, sep=".")
            if not flat.empty:
                raw_rows.append(flat.iloc[0].to_dict())
                success_count += 1
                print(f"Collected {success_count} enrichment rows")
            else:
                failed_count += 1
                print(f"Empty response for id={required['id']}")

        except requests.exceptions.RequestException as exc:
            failed_count += 1
            print(f"Failed for id={required['id']}: {exc}")

    return raw_rows, success_count, failed_count
