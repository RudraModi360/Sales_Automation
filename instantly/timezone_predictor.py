# pyright: reportMissingImports=false
import json
import os
from pydantic import BaseModel
from typing import List
import anthropic
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

AVAILABLE_TIMEZONES: tuple[str, ...] = (
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
	"Europe/Berlin",
	"Europe/London",
	"Europe/Paris",
	"Europe/Amsterdam",
	"Europe/Brussels",
	"Europe/Vienna",
	"Europe/Prague",
	"Europe/Warsaw",
	"Europe/Madrid",
	"Europe/Rome",
	"Europe/Athens",
	"Europe/Dublin",
	"Europe/Lisbon",
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

client = Anthropic()


class Timezone_prediction(BaseModel):
    timezone: str


def _resolve_model(client: Anthropic) -> str:
	requested = os.getenv("CLAUDE_LIGHT_MODEL", "claude-haiku-4-5-20251001").strip()
	if not requested:
		requested = "claude-haiku-4-5-20251001"

	try:
		models_page = client.models.list(limit=50)
		available_ids = [m.id for m in getattr(models_page, "data", []) if hasattr(m, "id")]
		if requested in available_ids:
			return requested
		for model_id in available_ids:
			if isinstance(model_id, str) and "claude" in model_id.lower():
				return model_id
	except Exception:
		pass

	return requested

def predict_timezone_with_claude(country: str) -> str | None:
	country = (country or "").strip()
	if not country:
		return "Asia/Kolkata"

	# Build strict prompt with ONLY allowed timezones
	allowed_list = "\n".join(AVAILABLE_TIMEZONES)
	
	model_id = _resolve_model(client)
	max_retries = 3
	
	for attempt in range(1, max_retries + 1):
		print(f"\n  [Timezone Prediction - Attempt {attempt}/{max_retries}]")
		
		if attempt == 1:
			# First attempt - strict instructions
			prompt = (
				f"Country: {country}\n\n"
				"CRITICAL INSTRUCTIONS:\n"
				"1. You MUST return EXACTLY ONE timezone from the list below\n"
				"2. Do NOT invent, modify, or suggest any timezone not in the list\n"
				"3. Do NOT suggest alternatives - pick the BEST match ONLY\n"
				"4. Return ONLY the timezone name, nothing else\n"
				"5. If unsure, pick the most common business timezone for that country\n\n"
				"ALLOWED TIMEZONES (you can ONLY choose from these):\n"
				f"{allowed_list}\n\n"
				"Your response format MUST be exactly: timezone_name\n"
				"Example: Europe/London\n"
				"Do not add explanation, quotes, or anything else."
			)
		else:
			# Retry attempts - with feedback from previous attempt
			prompt = (
				f"Country: {country}\n\n"
				f"PREVIOUS ATTEMPT ({attempt - 1}): You returned an INVALID timezone.\n"
				"You must pick from the STRICT list below - no exceptions.\n\n"
				"FINAL INSTRUCTIONS:\n"
				"• Return EXACTLY ONE timezone name from the list below\n"
				"• Do NOT invent timezones\n"
				"• Do NOT modify timezone names\n"
				"• Return format: timezone_name\n"
				"• Example: Europe/Berlin\n\n"
				"ALLOWED TIMEZONES (pick only from this list):\n"
				f"{allowed_list}\n\n"
				"Your response must be a timezone name from the list above. Nothing else."
			)
		
		try:
			response = client.messages.parse(
				model=model_id,
				max_tokens=50,
				output_format=Timezone_prediction,
				messages=[{"role": "user", "content": prompt}],
			)
			content = response.parsed_output
			predicted_tz = content.timezone if content else None
			
			print(f"  Claude Response: {predicted_tz}")
			
			# Validation
			if not predicted_tz:
				print(f"  ✗ Empty response - retrying...")
				if attempt == max_retries:
					print(f"  ✗ Max retries reached, using default")
					return "Asia/Kolkata"
				continue
			
			if predicted_tz not in AVAILABLE_TIMEZONES:
				print(f"  ✗ REJECTED: '{predicted_tz}' not in allowed list")
				matching = [tz for tz in AVAILABLE_TIMEZONES if country.lower() in tz.lower()]
				if matching:
					print(f"  Suggested options: {matching[:3]}")
				
				if attempt == max_retries:
					print(f"  ✗ Max retries reached, using default")
					return "Asia/Kolkata"
				print(f"  Retrying with feedback...")
				continue
			
			# Success!
			print(f"  ✓ ACCEPTED: {predicted_tz}")
			return predicted_tz
			
		except anthropic.APIError as e:
			print(f"  ✗ Claude API Error: {e}")
			if attempt == max_retries:
				print(f"  ✗ Max retries reached, using default")
				return "Asia/Kolkata"
			print(f"  Retrying...")
			continue
	
	# Fallback if all retries exhausted
	print(f"  ✗ All retry attempts exhausted, using default: Asia/Kolkata")
	return "Asia/Kolkata"
