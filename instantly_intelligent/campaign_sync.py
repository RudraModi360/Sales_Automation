from __future__ import annotations

import importlib
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Literal

import requests
from dotenv import load_dotenv


load_dotenv()

INSTANTLY_BASE_URL = "https://api.instantly.ai/api/v2"
DOC_SHAREPOINT_URL = os.getenv(
	"APOLLO_WORKBOOK_OUTPUT_URL",
	"https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/apollo_people_pipeline.xlsx",
)

ENV_BOOL_TRUE = {"1", "true", "yes", "y", "on"}
DEFAULT_TIMEZONE = "Africa/Abidjan"
MEDIUM_MATCH_THRESHOLD = 0.65
HIGH_MATCH_THRESHOLD = 0.85

# Allowed timezone set from user requirement / Instantly schedule options.
ALLOWED_TIMEZONES: tuple[str, ...] = (
	"Etc/GMT+12",
	"Etc/GMT+11",
	"Etc/GMT+10",
	"America/Anchorage",
	"America/Dawson",
	"America/Creston",
	"America/Chihuahua",
	"America/Boise",
	"America/Belize",
	"America/Chicago",
	"America/Bahia_Banderas",
	"America/Regina",
	"America/Bogota",
	"America/Detroit",
	"America/Indiana/Marengo",
	"America/Caracas",
	"America/Asuncion",
	"America/Glace_Bay",
	"America/Campo_Grande",
	"America/Anguilla",
	"America/Santiago",
	"America/St_Johns",
	"America/Sao_Paulo",
	"America/Argentina/La_Rioja",
	"America/Araguaina",
	"America/Godthab",
	"America/Montevideo",
	"America/Bahia",
	"America/Noronha",
	"America/Scoresbysund",
	"Atlantic/Cape_Verde",
	"Africa/Casablanca",
	"America/Danmarkshavn",
	"Europe/Isle_of_Man",
	"Atlantic/Canary",
	"Africa/Abidjan",
	"Arctic/Longyearbyen",
	"Europe/Belgrade",
	"Africa/Ceuta",
	"Europe/Sarajevo",
	"Africa/Algiers",
	"Africa/Windhoek",
	"Asia/Nicosia",
	"Asia/Beirut",
	"Africa/Cairo",
	"Asia/Damascus",
	"Europe/Bucharest",
	"Africa/Blantyre",
	"Europe/Helsinki",
	"Europe/Istanbul",
	"Asia/Jerusalem",
	"Africa/Tripoli",
	"Asia/Amman",
	"Asia/Baghdad",
	"Europe/Kaliningrad",
	"Asia/Aden",
	"Africa/Addis_Ababa",
	"Europe/Kirov",
	"Europe/Astrakhan",
	"Asia/Tehran",
	"Asia/Dubai",
	"Asia/Baku",
	"Indian/Mahe",
	"Asia/Tbilisi",
	"Asia/Yerevan",
	"Asia/Kabul",
	"Antarctica/Mawson",
	"Asia/Yekaterinburg",
	"Asia/Karachi",
	"Asia/Kolkata",
	"Asia/Colombo",
	"Asia/Kathmandu",
	"Antarctica/Vostok",
	"Asia/Dhaka",
	"Asia/Rangoon",
	"Antarctica/Davis",
	"Asia/Novokuznetsk",
	"Asia/Hong_Kong",
	"Asia/Krasnoyarsk",
	"Asia/Brunei",
	"Australia/Perth",
	"Asia/Taipei",
	"Asia/Choibalsan",
	"Asia/Irkutsk",
	"Asia/Dili",
	"Asia/Pyongyang",
	"Australia/Adelaide",
	"Australia/Darwin",
	"Australia/Brisbane",
	"Australia/Melbourne",
	"Antarctica/DumontDUrville",
	"Australia/Currie",
	"Asia/Chita",
	"Antarctica/Macquarie",
	"Asia/Sakhalin",
	"Pacific/Auckland",
	"Etc/GMT-12",
	"Pacific/Fiji",
	"Asia/Anadyr",
	"Asia/Kamchatka",
	"Etc/GMT-13",
	"Pacific/Apia",
)

COUNTRY_ALIASES: dict[str, set[str]] = {
	"united states": {"united states", "usa", "u.s.a", "us", "u.s", "america"},
	"united kingdom": {"united kingdom", "uk", "u.k", "great britain", "britain", "england", "gb"},
	"united arab emirates": {"uae", "u.a.e", "united arab emirates"},
	"south korea": {"south korea", "korea republic", "republic of korea", "korea"},
	"russia": {"russia", "russian federation"},
}

DETERMINISTIC_COUNTRY_TIMEZONE_MAP: dict[str, str] = {
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
	"denmark": "Europe/Belgrade",
	"egypt": "Africa/Cairo",
	"finland": "Europe/Helsinki",
	"france": "Europe/Belgrade",
	"germany": "Europe/Belgrade",
	"greece": "Europe/Helsinki",
	"india": "Asia/Kolkata",
	"ireland": "Europe/Isle_of_Man",
	"israel": "Asia/Jerusalem",
	"italy": "Europe/Belgrade",
	"mexico": "America/Chicago",
	"netherlands": "Europe/Belgrade",
	"new zealand": "Pacific/Auckland",
	"norway": "Europe/Belgrade",
	"pakistan": "Asia/Karachi",
	"poland": "Europe/Belgrade",
	"portugal": "Atlantic/Canary",
	"qatar": "Asia/Dubai",
	"romania": "Europe/Bucharest",
	"saudi arabia": "Asia/Aden",
	"singapore": "Asia/Hong_Kong",
	"south africa": "Africa/Blantyre",
	"spain": "Europe/Belgrade",
	"sweden": "Europe/Belgrade",
	"switzerland": "Europe/Belgrade",
	"taiwan": "Asia/Taipei",
	"turkey": "Europe/Istanbul",
	"united kingdom": "Europe/Isle_of_Man",
	"united states": "America/Chicago",
	"vietnam": "Asia/Hong_Kong",
}

IGNORED_CAMPAIGN_TOKENS = {
	"campaign",
	"outbound",
	"cold",
	"email",
	"sequence",
	"lead",
	"leads",
	"marketing",
}


@dataclass(slots=True)
class CampaignMatchResult:
	country_key: str
	matched: bool
	campaign_id: str | None
	campaign_name: str | None
	confidence: float
	method: str
	candidates: list[dict[str, Any]]
	decision_reason: str
	policy_action: Literal["use_existing", "create_new", "manual_review"]


@dataclass(slots=True)
class TimezoneDecision:
	timezone: str
	confidence: float
	source: Literal["observed", "deterministic_map", "ai", "default"]
	rationale: str
	valid_in_allowed: bool


def _env_bool(name: str, default: bool) -> bool:
	value = os.getenv(name)
	if value is None:
		return default
	return value.strip().lower() in ENV_BOOL_TRUE


def _normalize_text(value: Any) -> str:
	return str(value or "").strip().lower()


def _normalize_email(value: Any) -> str:
	email = _normalize_text(value)
	if not email or "@" not in email:
		return ""
	return email


def _clean_token_text(value: str) -> str:
	return re.sub(r"[^a-z0-9]+", " ", _normalize_text(value)).strip()


def _tokenize(value: str) -> set[str]:
	return {
		token
		for token in _clean_token_text(value).split()
		if token and token not in IGNORED_CAMPAIGN_TOKENS
	}


def _canonical_country_key(country: str) -> str:
	norm = _clean_token_text(country)
	if not norm:
		return "unknown"
	for canonical, aliases in COUNTRY_ALIASES.items():
		if norm == canonical or norm in aliases:
			return canonical
	return norm


def _canonicalize_campaign_name(name: str) -> str:
	cleaned = _clean_token_text(name)
	if not cleaned:
		return ""
	for canonical, aliases in COUNTRY_ALIASES.items():
		if cleaned == canonical or cleaned in aliases:
			return canonical
	return cleaned


def _campaign_score(country_key: str, campaign_name: str) -> tuple[float, str]:
	name_key = _canonicalize_campaign_name(campaign_name)
	if not name_key:
		return 0.0, "empty_campaign_name"

	if name_key == country_key:
		return 1.0, "exact_canonical_match"

	country_tokens = _tokenize(country_key)
	name_tokens = _tokenize(name_key)
	if not country_tokens or not name_tokens:
		return 0.0, "missing_tokens"

	intersection = len(country_tokens.intersection(name_tokens))
	union = len(country_tokens.union(name_tokens))
	jaccard = intersection / union if union else 0.0
	contains_bonus = 0.2 if country_key in name_key else 0.0
	prefix_bonus = 0.1 if name_key.startswith(country_key) else 0.0
	score = min(0.95, jaccard + contains_bonus + prefix_bonus)
	return score, "token_overlap"


def match_country_to_campaign(
	country: str,
	campaigns: list[dict[str, Any]],
	*,
	high_threshold: float = HIGH_MATCH_THRESHOLD,
	medium_threshold: float = MEDIUM_MATCH_THRESHOLD,
	allow_medium_confidence_bind: bool = False,
	auto_create_campaigns: bool = True,
) -> CampaignMatchResult:
	country_key = _canonical_country_key(country)
	scored: list[tuple[float, str, dict[str, Any]]] = []
	for campaign in campaigns:
		if not isinstance(campaign, dict):
			continue
		name = str(campaign.get("name", "") or "")
		score, method = _campaign_score(country_key, name)
		if score <= 0:
			continue
		scored.append((score, method, campaign))

	scored.sort(key=lambda item: item[0], reverse=True)
	candidates = [
		{
			"id": campaign.get("id"),
			"name": campaign.get("name"),
			"confidence": round(score, 4),
			"method": method,
		}
		for score, method, campaign in scored[:3]
	]

	if not scored:
		action: Literal["use_existing", "create_new", "manual_review"] = (
			"create_new" if auto_create_campaigns else "manual_review"
		)
		return CampaignMatchResult(
			country_key=country_key,
			matched=False,
			campaign_id=None,
			campaign_name=None,
			confidence=0.0,
			method="no_match",
			candidates=[],
			decision_reason="No campaign name matched country tokens/aliases.",
			policy_action=action,
		)

	best_score, best_method, best_campaign = scored[0]
	campaign_id = str(best_campaign.get("id") or "") or None
	campaign_name = str(best_campaign.get("name") or "") or None

	if best_score >= high_threshold and campaign_id:
		return CampaignMatchResult(
			country_key=country_key,
			matched=True,
			campaign_id=campaign_id,
			campaign_name=campaign_name,
			confidence=round(best_score, 4),
			method=best_method,
			candidates=candidates,
			decision_reason="High-confidence campaign match.",
			policy_action="use_existing",
		)

	if best_score >= medium_threshold:
		if allow_medium_confidence_bind and campaign_id:
			return CampaignMatchResult(
				country_key=country_key,
				matched=True,
				campaign_id=campaign_id,
				campaign_name=campaign_name,
				confidence=round(best_score, 4),
				method=best_method,
				candidates=candidates,
				decision_reason="Medium-confidence bind explicitly enabled.",
				policy_action="use_existing",
			)
		return CampaignMatchResult(
			country_key=country_key,
			matched=False,
			campaign_id=campaign_id,
			campaign_name=campaign_name,
			confidence=round(best_score, 4),
			method=best_method,
			candidates=candidates,
			decision_reason="Medium-confidence match; requires manual review or explicit override.",
			policy_action="manual_review",
		)

	action = "create_new" if auto_create_campaigns else "manual_review"
	return CampaignMatchResult(
		country_key=country_key,
		matched=False,
		campaign_id=campaign_id,
		campaign_name=campaign_name,
		confidence=round(best_score, 4),
		method=best_method,
		candidates=candidates,
		decision_reason="Low-confidence campaign match; auto-bind disabled.",
		policy_action=action,
	)


def _predict_timezone_with_existing_function(country: str) -> str | None:
	"""
	Use the existing predictor in instantly/timezone_predictor.py.
	Returns None if unavailable/failing.
	"""
	try:
		module = importlib.import_module("instantly.timezone_predictor")
		predictor = getattr(module, "predict_timezone_with_claude", None)
		if callable(predictor):
			result = predictor(country)
			if isinstance(result, str):
				value = result.strip()
				return value or None
	except Exception:
		return None
	return None


def resolve_country_timezone(
	country: str,
	leads: list[dict[str, Any]],
	*,
	use_ai_predictor: bool = True,
) -> TimezoneDecision:
	observed = [
		str(lead.get("timezone", "")).strip()
		for lead in leads
		if isinstance(lead, dict)
		and isinstance(lead.get("timezone"), str)
		and str(lead.get("timezone")).strip() in ALLOWED_TIMEZONES
	]
	if observed:
		winner = Counter(observed).most_common(1)[0][0]
		return TimezoneDecision(
			timezone=winner,
			confidence=1.0,
			source="observed",
			rationale="Most-common valid timezone from grouped leads.",
			valid_in_allowed=True,
		)

	country_key = _canonical_country_key(country)
	deterministic = DETERMINISTIC_COUNTRY_TIMEZONE_MAP.get(country_key)
	if deterministic in ALLOWED_TIMEZONES:
		return TimezoneDecision(
			timezone=deterministic,
			confidence=0.9,
			source="deterministic_map",
			rationale="Country alias matched deterministic timezone map.",
			valid_in_allowed=True,
		)

	if use_ai_predictor:
		predicted = _predict_timezone_with_existing_function(country)
		if predicted in ALLOWED_TIMEZONES:
			return TimezoneDecision(
				timezone=predicted,
				confidence=0.75,
				source="ai",
				rationale="Existing predictor returned an allowed timezone.",
				valid_in_allowed=True,
			)

	return TimezoneDecision(
		timezone=DEFAULT_TIMEZONE,
		confidence=0.2,
		source="default",
		rationale="No valid observed/deterministic/AI timezone available.",
		valid_in_allowed=True,
	)


def _chunk(items: list[dict[str, Any]], size: int = 1000) -> list[list[dict[str, Any]]]:
	return [items[i : i + size] for i in range(0, len(items), size)]


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


def _get_country_groups_from_sharepoint(sharepoint_url: str) -> dict[str, list[dict[str, Any]]]:
	try:
		module = importlib.import_module("instantly.country_based_grouping")
		group_fn = getattr(module, "get_country_grouped_output")
	except Exception as exc:  # noqa: BLE001
		raise RuntimeError("Unable to import country grouping from instantly.country_based_grouping.") from exc

	grouped_output = group_fn(sharepoint_url)
	return flatten_country_groups(grouped_output)


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
			"source": "intelligent_country_sync",
		},
	}

	for optional_key in ["phone", "website", "personalization"]:
		val = lead.get(optional_key)
		if isinstance(val, str) and val.strip():
			payload[optional_key] = val.strip()

	return payload


class InstantlyApiClient:
	def __init__(self, *, base_url: str = INSTANTLY_BASE_URL, api_key: str | None = None, max_attempts: int = 5):
		self.base_url = base_url.rstrip("/")
		self.api_key = (api_key or os.getenv("INSTANTLY_API_KEY", "")).strip()
		self.max_attempts = max_attempts
		if not self.api_key:
			raise ValueError("INSTANTLY_API_KEY is not set.")

	@property
	def headers(self) -> dict[str, str]:
		return {
			"Authorization": f"Bearer {self.api_key}",
			"Accept": "application/json",
			"Content-Type": "application/json",
		}

	def _request(
		self,
		method: str,
		path: str,
		*,
		params: dict[str, Any] | None = None,
		body: dict[str, Any] | None = None,
	) -> dict[str, Any]:
		url = f"{self.base_url}{path}"
		last_error: Exception | None = None
		for attempt in range(1, self.max_attempts + 1):
			try:
				response = requests.request(
					method=method,
					url=url,
					headers=self.headers,
					params=params,
					json=body,
					timeout=60,
				)
			except requests.RequestException as exc:
				last_error = exc
				if attempt == self.max_attempts:
					raise
				time.sleep(2 ** (attempt - 1))
				continue

			if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_attempts:
				time.sleep(2 ** (attempt - 1))
				continue

			if response.status_code in {401, 402, 403}:
				raise PermissionError(f"Instantly auth/plan/scope failure ({response.status_code}) on {path}.")

			response.raise_for_status()
			if not response.content:
				return {}
			try:
				payload = response.json()
			except ValueError:
				return {}
			return payload if isinstance(payload, dict) else {}

		if last_error:
			raise last_error
		raise RuntimeError(f"Request failed: {method} {path}")

	def list_campaigns(self) -> list[dict[str, Any]]:
		items: list[dict[str, Any]] = []
		starting_after: str | None = None
		while True:
			params: dict[str, Any] = {"limit": 100}
			if starting_after:
				params["starting_after"] = starting_after
			payload = self._request("GET", "/campaigns", params=params)
			page_items = payload.get("items", [])
			if isinstance(page_items, list):
				items.extend([item for item in page_items if isinstance(item, dict)])
			starting_after = payload.get("next_starting_after")
			if not starting_after:
				break
		return items

	def list_campaign_leads(self, campaign_id: str) -> list[dict[str, Any]]:
		items: list[dict[str, Any]] = []
		starting_after: str | None = None
		while True:
			body: dict[str, Any] = {"campaign": campaign_id, "limit": 100}
			if starting_after:
				body["starting_after"] = starting_after
			payload = self._request("POST", "/leads/list", body=body)
			page_items = payload.get("items", [])
			if isinstance(page_items, list):
				items.extend([item for item in page_items if isinstance(item, dict)])
			starting_after = payload.get("next_starting_after")
			if not starting_after:
				break
		return items

	def add_leads_to_campaign(self, campaign_id: str, leads: list[dict[str, Any]]) -> dict[str, Any]:
		body = {
			"campaign_id": campaign_id,
			"leads": leads,
			"skip_if_in_campaign": True,
			"skip_if_in_workspace": False,
			"verify_leads_on_import": False,
		}
		return self._request("POST", "/leads/add", body=body)

	def create_campaign(self, country: str, timezone: str) -> dict[str, Any]:
		body = {
			"name": f"{country.strip()} | Outbound",
			"campaign_schedule": {
				"schedules": [
					{
						"name": "Business Hours",
						"timing": {"from": "09:00", "to": "17:00"},
						"days": {
							"0": True,
							"1": True,
							"2": True,
							"3": True,
							"4": True,
							"5": False,
							"6": False,
						},
						"timezone": timezone,
					}
				]
			},
		}
		return self._request("POST", "/campaigns", body=body)


def run_sync(
	*,
	sharepoint_url: str = DOC_SHAREPOINT_URL,
	country_groups: dict[str, list[dict[str, Any]]] | None = None,
	dry_run: bool | None = None,
	auto_create_campaigns: bool | None = None,
	allow_medium_confidence_bind: bool | None = None,
	use_ai_timezone_predictor: bool | None = None,
	client: InstantlyApiClient | Any | None = None,
) -> list[dict[str, Any]]:
	dry_run = _env_bool("INSTANTLY_DRY_RUN", True) if dry_run is None else dry_run
	auto_create_campaigns = (
		_env_bool("INSTANTLY_AUTO_CREATE_CAMPAIGNS", True)
		if auto_create_campaigns is None
		else auto_create_campaigns
	)
	allow_medium_confidence_bind = (
		_env_bool("INSTANTLY_MEDIUM_CONFIDENCE_BIND", False)
		if allow_medium_confidence_bind is None
		else allow_medium_confidence_bind
	)
	use_ai_timezone_predictor = (
		_env_bool("CLAUDE_TIMEZONE_ENABLED", True)
		if use_ai_timezone_predictor is None
		else use_ai_timezone_predictor
	)

	if country_groups is None:
		country_groups = _get_country_groups_from_sharepoint(sharepoint_url)

	client = client or InstantlyApiClient()
	campaigns = client.list_campaigns()
	run_report: list[dict[str, Any]] = []

	for country, leads in sorted(country_groups.items(), key=lambda x: x[0].lower()):
		match = match_country_to_campaign(
			country,
			campaigns,
			allow_medium_confidence_bind=allow_medium_confidence_bind,
			auto_create_campaigns=auto_create_campaigns,
		)
		timezone_decision = resolve_country_timezone(
			country,
			leads,
			use_ai_predictor=use_ai_timezone_predictor,
		)

		entry: dict[str, Any] = {
			"country": country,
			"country_key": match.country_key,
			"input_leads": len(leads),
			"match_confidence": match.confidence,
			"match_method": match.method,
			"policy_action": match.policy_action,
			"decision_reason": match.decision_reason,
			"top_candidates": match.candidates,
			"timezone": timezone_decision.timezone,
			"timezone_source": timezone_decision.source,
			"timezone_confidence": timezone_decision.confidence,
			"campaign_name": match.campaign_name,
			"campaign_id": match.campaign_id,
			"matched_campaign_id": match.campaign_id,
			"matched_campaign_name": match.campaign_name,
			"campaign_created": False,
			"existing_campaign_leads": 0,
			"missing_leads": 0,
			"uploaded": 0,
			"skipped": 0,
			"errors": [],
		}

		campaign_id = match.campaign_id if match.policy_action == "use_existing" else None
		if match.policy_action == "manual_review":
			entry["errors"].append("manual_review_required_for_campaign_selection")
			run_report.append(entry)
			continue

		if not campaign_id:
			if not auto_create_campaigns:
				entry["errors"].append("campaign_not_found_and_auto_create_disabled")
				run_report.append(entry)
				continue
			if dry_run:
				campaign_id = "DRY_RUN_CREATED_CAMPAIGN"
				entry["campaign_id"] = campaign_id
				entry["campaign_name"] = f"{country} | Outbound"
				entry["campaign_created"] = True
			else:
				try:
					created = client.create_campaign(country, timezone_decision.timezone)
					campaign_id = str(created.get("id") or "")
					if not campaign_id:
						raise ValueError("create_campaign returned no id")
					entry["campaign_id"] = campaign_id
					entry["campaign_name"] = created.get("name", f"{country} | Outbound")
					entry["campaign_created"] = True
				except Exception as exc:  # noqa: BLE001
					entry["errors"].append(f"campaign_create_failed: {exc}")
					run_report.append(entry)
					continue

		try:
			if dry_run and campaign_id == "DRY_RUN_CREATED_CAMPAIGN":
				existing_campaign_leads = []
			else:
				existing_campaign_leads = client.list_campaign_leads(campaign_id)
		except Exception as exc:  # noqa: BLE001
			entry["errors"].append(f"list_campaign_leads_failed: {exc}")
			run_report.append(entry)
			continue

		existing_emails = {
			_normalize_email(lead.get("email"))
			for lead in existing_campaign_leads
			if isinstance(lead, dict) and _normalize_email(lead.get("email"))
		}
		entry["existing_campaign_leads"] = len(existing_emails)

		upload_payload: list[dict[str, Any]] = []
		for lead in leads:
			payload = build_upload_lead(lead, country, timezone_decision.timezone)
			if not payload:
				continue
			if payload["email"] in existing_emails:
				continue
			upload_payload.append(payload)

		entry["missing_leads"] = len(upload_payload)
		if not upload_payload:
			run_report.append(entry)
			continue

		if dry_run:
			entry["uploaded"] = len(upload_payload)
			run_report.append(entry)
			continue

		for batch in _chunk(upload_payload, 1000):
			try:
				result = client.add_leads_to_campaign(campaign_id, batch)
				entry["uploaded"] += int(result.get("leads_uploaded", 0))
				entry["skipped"] += int(result.get("skipped_count", 0))
			except Exception as exc:  # noqa: BLE001
				entry["errors"].append(f"lead_upload_failed: {exc}")

		run_report.append(entry)

	return run_report


def main() -> None:
	report = run_sync()
	print(json.dumps(report, indent=2))


if __name__ == "__main__":
	main()
