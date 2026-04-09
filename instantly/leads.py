import os
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


LEAD_TOP_LEVEL_FIELDS = {
	"email",
	"personalization",
	"website",
	"last_name",
	"first_name",
	"company_name",
	"job_title",
	"phone",
	"lt_interest_status",
	"pl_value_lead",
	"assigned_to",
}


PRIMARY_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
	"email": ("email",),
	"first_name": ("first_name",),
	"last_name": ("last_name",),
	"company_name": ("company_name",),
	"job_title": ("job_title", "headline"),
	"website": ("website", "company_website"),
	"phone": ("phone", "company_phone"),
	"personalization": ("personalization",),
	"lt_interest_status": ("lt_interest_status",),
	"pl_value_lead": ("pl_value_lead",),
	"assigned_to": ("assigned_to",),
}


def _is_nullish(value: Any) -> bool:
	if value is None:
		return True
	try:
		return bool(pd.isna(value))
	except Exception:
		return False


def _clean_value(value: Any) -> Any:
	if _is_nullish(value):
		return None

	if isinstance(value, str):
		value = value.strip()
		return value if value else None

	return value


def _normalize_custom_value(value: Any) -> Any:
	cleaned = _clean_value(value)
	if cleaned is None:
		return None

	if isinstance(cleaned, (str, int, float, bool)):
		return cleaned

	return str(cleaned)


def _pick_first_non_empty(lead_dict: dict[str, Any], keys: tuple[str, ...]) -> tuple[Any, str | None]:
	for key in keys:
		value = _clean_value(lead_dict.get(key))
		if value is not None:
			return value, key

	return None, None


def _build_personalization_from_generated_series(lead_dict: dict[str, Any]) -> str | None:
	parts = [
		_clean_value(lead_dict.get("main_email_introduction")),
		_clean_value(lead_dict.get("main_email_value_proposition")),
		_clean_value(lead_dict.get("main_email_call_to_action")),
	]

	text_parts = [part for part in parts if isinstance(part, str) and part.strip()]
	if not text_parts:
		return None

	return "\n\n".join(text_parts)


def build_lead_payload_from_series(lead_series: pd.Series) -> dict[str, Any]:
	"""
	Convert a merged pandas Series (record + generated email fields) into
	a valid Instantly lead object.
	"""
	if not isinstance(lead_series, pd.Series):
		raise ValueError("lead_series must be a pandas Series")

	lead_dict: dict[str, Any] = {}
	for key, value in lead_series.items():
		if isinstance(key, str):
			lead_dict[key] = _clean_value(value)

	payload: dict[str, Any] = {}
	used_source_keys: set[str] = set()

	for target_field, aliases in PRIMARY_FIELD_ALIASES.items():
		value, source_key = _pick_first_non_empty(lead_dict, aliases)
		if value is None:
			continue

		payload[target_field] = value
		if source_key is not None:
			used_source_keys.add(source_key)

	if "personalization" not in payload:
		generated_personalization = _build_personalization_from_generated_series(lead_dict)
		if generated_personalization:
			payload["personalization"] = generated_personalization

	custom_variables: dict[str, Any] = {}
	for key, value in lead_dict.items():
		if key in LEAD_TOP_LEVEL_FIELDS or key in used_source_keys:
			continue

		normalized = _normalize_custom_value(value)
		if normalized is not None:
			custom_variables[key] = normalized

	if custom_variables:
		payload["custom_variables"] = custom_variables

	return payload


def upload_series_lead_to_campaign(
	lead_series: pd.Series,
	campaign_id: str,
	api_key: str = None,
	base_url: str = None,
	timeout: int = 60,
	skip_if_in_workspace: bool = True,
	verify_leads_on_import: bool = False,
	skip_if_in_campaign: bool = False,
	skip_if_in_list: bool = False,
	blocklist_id: str = None,
	assigned_to: str = None,
) -> dict[str, Any]:
	"""
	Upload one generated lead series to a specific campaign in Instantly.

	Args:
		lead_series: Combined pandas Series like pd.concat([record, result])
		campaign_id: Target Instantly campaign UUID

	Returns:
		Dict response from Instantly API or error object.
	"""
	if not isinstance(campaign_id, str) or not campaign_id.strip():
		raise ValueError("campaign_id is required and must be a non-empty string")

	if api_key is None:
		api_key = os.getenv("INSTANTLY_API_KEY")
		if not api_key:
			raise ValueError("API key is required. Set INSTANTLY_API_KEY in .env or pass as argument")

	if base_url is None:
		base_url = os.getenv("INSTANTLY_BASE_URL")
		if not base_url:
			raise ValueError("Base URL is required. Set INSTANTLY_BASE_URL in .env or pass as argument")

	lead_payload = build_lead_payload_from_series(lead_series)
	email = lead_payload.get("email")
	if not isinstance(email, str) or not email.strip():
		raise ValueError(
			"Lead email is required for campaign imports. Ensure final_series contains a valid email field."
		)

	request_payload: dict[str, Any] = {
		"campaign_id": campaign_id.strip(),
		"leads": [lead_payload],
		"verify_leads_on_import": verify_leads_on_import,
		"skip_if_in_workspace": skip_if_in_workspace,
		"skip_if_in_campaign": skip_if_in_campaign,
		"skip_if_in_list": skip_if_in_list,
	}

	if blocklist_id is not None:
		request_payload["blocklist_id"] = blocklist_id

	if assigned_to is not None:
		request_payload["assigned_to"] = assigned_to

	headers = {"Authorization": f"Bearer {api_key}"}
	url = base_url.rstrip("/") + "/api/v2/leads/add"
	response = requests.post(url, headers=headers, json=request_payload, timeout=timeout)

	if response.status_code == 200:
		return response.json()

	print(f"Error uploading lead to campaign: {response.status_code} - {response.text}")
	return {
		"error": f"{response.status_code} - {response.text}",
		"request_payload": request_payload,
	}
