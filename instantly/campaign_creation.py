import json
import os
import time
from collections import Counter
from typing import Any

import requests
from dotenv import load_dotenv

from country_based_grouping import get_country_grouped_output
from timezone_predictor import AVAILABLE_TIMEZONES, predict_timezone_with_claude


load_dotenv()

INSTANTLY_BASE_URL = "https://api.instantly.ai/api/v2"
DOC_SHAREPOINT_URL = os.getenv(
	"APOLLO_WORKBOOK_OUTPUT_URL",
	"https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/apollo_people_pipeline.xlsx",
)

# Fallback for countries without explicit mapping.
DEFAULT_TIMEZONE = "Africa/Abidjan"

STATIC_COUNTRY_TIMEZONE_MAP: dict[str, str] = {
	"argentina": "America/Argentina/La_Rioja",
	"australia": "Australia/Melbourne",
	"austria": "Europe/Belgrade",
	"bangladesh": "Asia/Dhaka",
	"belgium": "Europe/Belgrade",
	"brazil": "America/Sao_Paulo",
	"canada": "America/Detroit",
	"chile": "America/Santiago",
	"china": "Asia/Hong_Kong",
	"colombia": "America/Bogota",
	"czech republic": "Europe/Belgrade",
	"denmark": "Europe/Belgrade",
	"egypt": "Africa/Cairo",
	"finland": "Europe/Helsinki",
	"france": "Europe/Belgrade",
	"germany": "Europe/Belgrade",
	"greece": "Europe/Helsinki",
	"hong kong": "Asia/Hong_Kong",
	"hungary": "Europe/Belgrade",
	"india": "Asia/Kolkata",
	"indonesia": "Asia/Hong_Kong",
	"ireland": "Europe/Isle_of_Man",
	"israel": "Asia/Jerusalem",
	"italy": "Europe/Belgrade",
	"japan": "Asia/Tokyo",
	"kenya": "Africa/Addis_Ababa",
	"malaysia": "Asia/Hong_Kong",
	"mexico": "America/Chicago",
	"netherlands": "Europe/Belgrade",
	"new zealand": "Pacific/Auckland",
	"nigeria": "Africa/Algiers",
	"norway": "Europe/Belgrade",
	"pakistan": "Asia/Karachi",
	"philippines": "Asia/Hong_Kong",
	"poland": "Europe/Belgrade",
	"portugal": "Atlantic/Canary",
	"qatar": "Asia/Dubai",
	"romania": "Europe/Bucharest",
	"russia": "Europe/Kaliningrad",
	"saudi arabia": "Asia/Aden",
	"singapore": "Asia/Hong_Kong",
	"south africa": "Africa/Blantyre",
	"south korea": "Asia/Pyongyang",
	"spain": "Europe/Belgrade",
	"sweden": "Europe/Belgrade",
	"switzerland": "Europe/Belgrade",
	"taiwan": "Asia/Taipei",
	"thailand": "Asia/Bangkok",
	"turkey": "Europe/Istanbul",
	"uae": "Asia/Dubai",
	"uk": "Europe/Isle_of_Man",
	"united kingdom": "Europe/Isle_of_Man",
	"united states": "America/Chicago",
	"usa": "America/Chicago",
	"vietnam": "Asia/Hong_Kong",
}

ENV_BOOL_TRUE = {"1", "true", "yes", "y", "on"}


def _env_bool(name: str, default: bool) -> bool:
	value = os.getenv(name)
	if value is None:
		return default
	return value.strip().lower() in ENV_BOOL_TRUE


def _normalize_text(value: Any) -> str:
	if value is None:
		return ""
	return str(value).strip().lower()


def _normalize_email(value: Any) -> str:
	email = _normalize_text(value)
	if not email or "@" not in email:
		return ""
	return email


def _chunk(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
	return [items[i : i + size] for i in range(0, len(items), size)]


def _request_with_retry(
	method: str,
	url: str,
	headers: dict[str, str],
	params: dict[str, Any] | None = None,
	json_payload: dict[str, Any] | None = None,
	max_attempts: int = 5,
) -> requests.Response:
	last_error: Exception | None = None
	for attempt in range(1, max_attempts + 1):
		try:
			response = requests.request(
				method=method,
				url=url,
				headers=headers,
				params=params,
				json=json_payload,
				timeout=60,
			)
		except requests.RequestException as exc:
			last_error = exc
			if attempt == max_attempts:
				raise
			time.sleep(2 ** (attempt - 1))
			continue

		if response.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts:
			time.sleep(2 ** (attempt - 1))
			continue

		if response.status_code in {401, 402, 403}:
			msg = (
				f"Instantly API returned {response.status_code} for {method} {url}. "
				"Check API key, plan, and endpoint scopes."
			)
			print(f"[ERROR] {msg}")
			print(f"[RESPONSE] {response.text[:500]}")
			raise PermissionError(msg)

		if response.status_code >= 400:
			error_msg = f"{response.status_code} Error for {method} {url}"
			print(f"[ERROR] {error_msg}")
			print(f"[RESPONSE] {response.text[:800]}")
			response.raise_for_status()

		return response

	if last_error is not None:
		raise last_error
	raise RuntimeError(f"Failed request after retries: {method} {url}")


def _get_headers() -> dict[str, str]:
	api_key = os.getenv("INSTANTLY_API_KEY")
	if not api_key:
		raise ValueError("INSTANTLY_API_KEY is not set in environment variables.")
	return {
		"Authorization": f"Bearer {api_key}",
		"Accept": "application/json",
		"Content-Type": "application/json",
	}


def list_all_instantly_campaigns() -> list[dict[str, Any]]:
	headers = _get_headers()
	url = f"{INSTANTLY_BASE_URL}/campaigns"
	items: list[dict[str, Any]] = []
	starting_after: str | None = None
	page_count = 0

	while True:
		params: dict[str, Any] = {"limit": 100}
		if starting_after:
			params["starting_after"] = starting_after

		print(f"[API] GET /campaigns - Fetching campaigns list (page: {page_count + 1})")
		response = _request_with_retry("GET", url, headers, params=params)
		print(f"[API] Response status: {response.status_code}")
		payload = response.json()
		print(f"[API] Response body: {json.dumps(payload, indent=2)[:500]}")
		page_items = payload.get("items", []) if isinstance(payload, dict) else []
		if not isinstance(page_items, list):
			page_items = []

		items.extend([item for item in page_items if isinstance(item, dict)])
		starting_after = payload.get("next_starting_after") if isinstance(payload, dict) else None
		page_count += 1
		if not starting_after:
			break

	print(f"[API] Total campaigns fetched: {len(items)} across {page_count} page(s)")
	return items


def list_campaign_leads(campaign_id: str) -> list[dict[str, Any]]:
	headers = _get_headers()
	url = f"{INSTANTLY_BASE_URL}/leads/list"
	items: list[dict[str, Any]] = []
	starting_after: str | None = None
	page_count = 0

	while True:
		body: dict[str, Any] = {"campaign": campaign_id, "limit": 100}
		if starting_after:
			body["starting_after"] = starting_after

		print(f"    [API] POST /leads/list - Fetching campaign leads (campaign_id: {campaign_id}, page: {page_count + 1})")
		response = _request_with_retry("POST", url, headers, json_payload=body)
		print(f"    [API] Response status: {response.status_code}")
		payload = response.json()
		print(f"    [RESPONSE] Items count: {len(payload.get('items', []))}")
		page_items = payload.get("items", []) if isinstance(payload, dict) else []
		if not isinstance(page_items, list):
			page_items = []

		items.extend([item for item in page_items if isinstance(item, dict)])
		starting_after = payload.get("next_starting_after") if isinstance(payload, dict) else None
		page_count += 1
		if not starting_after:
			break

	print(f"    [API] Total leads fetched: {len(items)} across {page_count} page(s)")
	return items


def add_leads_to_campaign(campaign_id: str, leads: list[dict[str, Any]]) -> dict[str, Any]:
	headers = _get_headers()
	url = f"{INSTANTLY_BASE_URL}/leads/add"
	body = {
		"campaign_id": campaign_id,
		"leads": leads,
		"skip_if_in_campaign": True,
		"skip_if_in_workspace": False,
		"verify_leads_on_import": False,
	}
	print(f"      [API] POST /leads/add - Uploading {len(leads)} leads to campaign {campaign_id}")
	print(f"      [REQUEST] Payload (leads sample): {json.dumps(body['leads'][:1], indent=8) if body['leads'] else '[]'}")
	response = _request_with_retry("POST", url, headers, json_payload=body)
	print(f"      [API] Response status: {response.status_code}")
	result = response.json() if response.content else {}
	print(f"      [RESPONSE] Body: {json.dumps(result, indent=8)[:600]}")
	return result


def create_campaign(country: str, timezone: str) -> dict[str, Any]:
	headers = _get_headers()
	url = f"{INSTANTLY_BASE_URL}/campaigns"
	body = {
		"name": country,
		"campaign_schedule": {
			"schedules": [
				{
					"name": "Business Hours",
					"timing": {"from": "09:00", "to": "17:00"},
					"days": {
						"monday": True,
						"tuesday": True,
						"wednesday": True,
						"thursday": True,
						"friday": True,
						"saturday": False,
						"sunday": False,
					},
					"timezone": timezone,
				}
			]
		},
	}
	print(f"    [API] POST /campaigns - Creating campaign")
	print(f"    [REQUEST] Payload: {json.dumps(body, indent=6)}")
	response = _request_with_retry("POST", url, headers, json_payload=body)
	print(f"    [API] Response status: {response.status_code}")
	result = response.json() if response.content else {}
	print(f"    [RESPONSE] Body: {json.dumps(result, indent=6)[:800]}")
	return result


def flatten_country_groups(grouped_output: list[dict[str, list[dict[str, Any]]]]) -> dict[str, list[dict[str, Any]]]:
	result: dict[str, list[dict[str, Any]]] = {}
	for item in grouped_output:
		if not isinstance(item, dict):
			continue
		for country, leads in item.items():
			country_key = str(country).strip() or "Unknown"
			if not isinstance(leads, list):
				continue
			result[country_key] = [lead for lead in leads if isinstance(lead, dict)]
	return result


def campaign_match_score(country: str, campaign_name: str) -> int:
	country_norm = _normalize_text(country)
	name_norm = _normalize_text(campaign_name)
	if not country_norm or not name_norm:
		return -1
	if country_norm == name_norm:
		return 3
	if name_norm.startswith(country_norm):
		return 2
	if country_norm in name_norm:
		return 1
	return -1


def find_matching_campaign(country: str, campaigns: list[dict[str, Any]]) -> dict[str, Any] | None:
	best: dict[str, Any] | None = None
	best_score = -1
	for campaign in campaigns:
		name = campaign.get("name", "")
		score = campaign_match_score(country, name)
		if score > best_score:
			best_score = score
			best = campaign
	
	if best_score >= 0:
		pass
	else:
		pass
	
	return best


def resolve_country_timezone(country: str) -> str:
	timezone = predict_timezone_with_claude(country)
	return timezone if timezone else "Asia/Kolkata"


def build_upload_lead(lead: dict[str, Any], country: str, timezone: str) -> dict[str, Any] | None:
	email = _normalize_email(lead.get("email"))
	if not email:
		return None

	payload: dict[str, Any] = {
		"email": email,
		"first_name": str(lead.get("first_name", "") or "").strip(),
		"last_name": str(lead.get("last_name", "") or "").strip(),
		"company_name": str(lead.get("company_name", "") or "").strip(),
		"custom_variables": {
			"country": country,
			"timezone": timezone,
			"source": "sharepoint_summary_sync",
		},
	}

	for optional_key in ["phone", "website", "personalization"]:
		val = lead.get(optional_key)
		if isinstance(val, str) and val.strip():
			payload[optional_key] = val.strip()

	return payload


def run_sync() -> None:
	auto_create_campaigns = _env_bool("INSTANTLY_AUTO_CREATE_CAMPAIGNS", True)

	print("Fetching and grouping data by country from SharePoint summary...")
	grouped_output = get_country_grouped_output(DOC_SHAREPOINT_URL)
	country_groups = flatten_country_groups(grouped_output)
	print(f"Country groups found: {len(country_groups)}")

	print("Listing Instantly campaigns...")
	campaigns = list_all_instantly_campaigns()
	print(f"Total campaigns retrieved: {len(campaigns)}")

	run_report: list[dict[str, Any]] = []

	for country, leads in sorted(country_groups.items(), key=lambda x: x[0].lower()):
		print(f"\n{'='*70}")
		print(f"Processing country: {country} | Input leads: {len(leads)}")
		print(f"{'='*70}")

		matched_campaign = find_matching_campaign(country, campaigns)
		timezone = None

		entry: dict[str, Any] = {
			"country": country,
			"timezone": None,
			"input_leads": len(leads),
			"campaign_name": None,
			"campaign_id": None,
			"campaign_created": False,
			"existing_campaign_leads": 0,
			"missing_leads": 0,
			"uploaded": 0,
			"skipped": 0,
			"errors": [],
		}

		campaign_id = None

		if matched_campaign:
			# Campaign exists - no need to predict timezone
			campaign_id = matched_campaign.get("id")
			campaign_name = matched_campaign.get("name")
			entry["campaign_name"] = campaign_name
			entry["campaign_id"] = campaign_id
			timezone = matched_campaign.get("timezone", "")
			print(f"✓ Campaign FOUND: '{campaign_name}' (ID: {campaign_id})")
			print(f"  Timezone from campaign: {timezone}")
		else:
			# Campaign doesn't exist - predict timezone and create it
			print(f"✗ Campaign NOT FOUND - attempting to create...")
			if auto_create_campaigns:
				print(f"  Predicting timezone for country: {country}")
				timezone = resolve_country_timezone(country)
				entry["timezone"] = timezone
				print(f"  ✓ Predicted timezone: {timezone}")

				try:
					print(f"  Creating campaign '{country}' with timezone '{timezone}'...")
					created = create_campaign(country, timezone)
					campaign_id = created.get("id")
					entry["campaign_created"] = True
					entry["campaign_name"] = created.get("name", country)
					entry["campaign_id"] = campaign_id
					print(f"  ✓ Campaign created successfully (ID: {campaign_id})")
				except Exception as exc:  # noqa: BLE001
					entry["errors"].append(f"campaign_create_failed: {exc}")
					print(f"  ✗ Campaign creation failed: {exc}")
			else:
				entry["errors"].append("campaign_not_found_and_auto_create_disabled")
				print(f"  ✗ Auto-create disabled, skipping...")

		if not campaign_id:
			print(f"⚠ No campaign_id available, skipping country {country}")
			run_report.append(entry)
			continue

		try:
			print(f"  Fetching existing leads from campaign (ID: {campaign_id})...")
			existing_campaign_leads = list_campaign_leads(campaign_id)
			print(f"  ✓ Fetched {len(existing_campaign_leads)} existing leads")
		except Exception as exc:  # noqa: BLE001
			entry["errors"].append(f"list_campaign_leads_failed: {exc}")
			print(f"  ✗ Failed to fetch campaign leads: {exc}")
			run_report.append(entry)
			continue

		existing_emails = {
			_normalize_email(lead.get("email"))
			for lead in existing_campaign_leads
			if _normalize_email(lead.get("email"))
		}
		entry["existing_campaign_leads"] = len(existing_emails)
		print(f"  Existing unique emails in campaign: {len(existing_emails)}")

		upload_payload: list[dict[str, Any]] = []
		skipped_invalid = 0
		skipped_duplicate = 0

		for lead in leads:
			lead_payload = build_upload_lead(lead, country, timezone or "")
			if not lead_payload:
				skipped_invalid += 1
				continue
			if lead_payload["email"] in existing_emails:
				skipped_duplicate += 1
				continue
			upload_payload.append(lead_payload)

		entry["missing_leads"] = len(upload_payload)
		print(f"  Leads to upload: {len(upload_payload)} | Invalid: {skipped_invalid} | Already exists: {skipped_duplicate}")

		if not upload_payload:
			print(f"  No new leads to upload for {country}, moving to next...")
			run_report.append(entry)
			continue

		batch_count = len(list(_chunk(upload_payload, 1000)))
		print(f"  Uploading {len(upload_payload)} leads in {batch_count} batch(es)...")

		for batch_idx, batch in enumerate(_chunk(upload_payload, 1000), 1):
			try:
				print(f"    Batch {batch_idx}: Uploading {len(batch)} leads...")
				result = add_leads_to_campaign(campaign_id, batch)
				uploaded = int(result.get("leads_uploaded", 0))
				skipped = int(result.get("skipped_count", 0))
				entry["uploaded"] += uploaded
				entry["skipped"] += skipped
				print(f"    ✓ Batch {batch_idx} result: {uploaded} uploaded, {skipped} skipped")
			except Exception as exc:  # noqa: BLE001
				entry["errors"].append(f"lead_upload_failed: {exc}")
				print(f"    ✗ Batch {batch_idx} failed: {exc}")

		print(f"✓ Country {country} complete: {entry['uploaded']} total uploaded, {entry['skipped']} skipped")
		run_report.append(entry)

	print(f"\n{'='*70}")
	print("SYNC COMPLETE - FINAL SUMMARY")
	print(f"{'='*70}")

	total_countries = len(run_report)
	total_input_leads = sum(e["input_leads"] for e in run_report)
	total_uploaded = sum(e["uploaded"] for e in run_report)
	total_skipped = sum(e["skipped"] for e in run_report)
	total_existing = sum(e["existing_campaign_leads"] for e in run_report)
	campaigns_created = sum(1 for e in run_report if e["campaign_created"])
	errors_count = sum(len(e["errors"]) for e in run_report)

	print(f"Countries processed: {total_countries}")
	print(f"Total input leads: {total_input_leads}")
	print(f"Campaigns created: {campaigns_created}")
	print(f"Leads already in campaigns: {total_existing}")
	print(f"Leads uploaded: {total_uploaded}")
	print(f"Leads skipped: {total_skipped}")
	print(f"Total errors: {errors_count}")
	print(f"{'='*70}\n")

	print("Detailed Report:")
	print(json.dumps(run_report, indent=2))


if __name__ == "__main__":
	run_sync()