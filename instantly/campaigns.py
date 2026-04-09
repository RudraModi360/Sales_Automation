import os
import re
import requests
import textwrap
from dotenv import load_dotenv

try:
    from .email_template import email_format, get_subject_line
    from .timezone_prediction import is_valid_timezone, predict_timezone
except ImportError:
    from email_template import email_format, get_subject_line
    from timezone_prediction import is_valid_timezone, predict_timezone

load_dotenv()


_BR_TAG_PATTERN = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)
_TEMPLATE_VARIABLE_PATTERN = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")
_LINE_BLOCK_STYLE = "margin:0;padding:0;line-height:1.2;"


def _normalize_template_body(raw_body: str) -> str:
    """
    Normalize email body for Instantly and preserve visual line breaks without <br>.

    Why: Instantly may drop/ignore <br> tags in campaign create payloads.
    We convert the template into explicit HTML block lines (<div>) so the
    sequence editor and sent emails keep the intended structure.
    """
    body = textwrap.dedent(str(raw_body or "")).strip()
    body = body.replace("\r\n", "\n").replace("\r", "\n")
    
    if _BR_TAG_PATTERN.search(body):
        body = re.sub(r"\n\s*", "", body)

    body = _BR_TAG_PATTERN.sub("\n", body)

    lines = [line.strip() for line in body.split("\n")]

    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    if not lines:
        return ""

    normalized_lines: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line
        if is_blank:
            if previous_blank:
                continue
            normalized_lines.append(f"<div style=\"{_LINE_BLOCK_STYLE}\">&nbsp;</div>")
        else:
            normalized_lines.append(f"<div style=\"{_LINE_BLOCK_STYLE}\">{line}</div>")

        previous_blank = is_blank

    return "".join(normalized_lines)


def _get_campaign_step_bodies(campaign_data: dict) -> list[str]:
    sequences = campaign_data.get("sequences")
    if not isinstance(sequences, list) or not sequences:
        return []

    first_sequence = sequences[0]
    if not isinstance(first_sequence, dict):
        return []

    steps = first_sequence.get("steps")
    if not isinstance(steps, list):
        return []

    bodies: list[str] = []
    for step in steps:
        if not isinstance(step, dict):
            bodies.append("")
            continue

        variants = step.get("variants")
        if not isinstance(variants, list) or not variants:
            bodies.append("")
            continue

        first_variant = variants[0]
        if not isinstance(first_variant, dict):
            bodies.append("")
            continue

        bodies.append(str(first_variant.get("body", "") or ""))

    return bodies


def _is_timezone_validation_error(status_code: int, response_text: str) -> bool:
    if status_code != 400:
        return False

    lowered = (response_text or "").lower()
    return "timezone" in lowered and "allowed values" in lowered


def list_campaigns(api_key: None = None, base_url: None = None, timeout: int = 60) -> dict:
    """
    List campaigns in Apollo.io using campaigns API endpoint.
    
    Args:
        api_key (str): Apollo API key
        
    """
    if api_key is None:
        if os.getenv('INSTANTLY_API_KEY'):
            api_key = os.getenv('INSTANTLY_API_KEY')
        else:
            raise ValueError("API key is required. Set INSTANTLY_API_KEY  in .env or pass as argument")
    
    if base_url is None:
        if os.getenv('INSTANTLY_BASE_URL'):
            base_url = os.getenv('INSTANTLY_BASE_URL')
        else:
            raise ValueError("Base URL is required. Set INSTANTLY_BASE_URL in .env or pass as argument")
    
    url = base_url+"/api/v2/campaigns"

    headers = {"Authorization": f"Bearer {api_key}"}

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching campaigns: {response.status_code} - {response.text}")
        return {"error": f"{response.status_code} - {response.text}", "campaigns": []}

def extract_country_names(campaign_response: dict) -> list[str]:
    """
    Extract campaign names (country names) from Instantly campaigns response.

    Args:
        campaign_response (dict): API response containing an `items` list

    Returns:
        list[str]: Country names from each campaign item
    """
    items = campaign_response.get("items", [])
    if not isinstance(items, list):
        return []

    country_names: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        country_name = item.get("name")
        if isinstance(country_name, str) and country_name.strip():
            country_names.append(country_name.strip())

    return country_names

def start_campaign(campaign_id: str, api_key: str = None, base_url: str = None, timeout: int = 60) -> dict:
    """
    Start (activate) a campaign in Instantly.ai using the campaigns API endpoint.
    
    Args:
        campaign_id (str): The campaign ID to activate
        api_key (str): Instantly API key
        base_url (str): Instantly API base URL
        timeout (int): Request timeout in seconds
        
    Returns:
        dict: Response from the API
    """
    if api_key is None:
        if os.getenv('INSTANTLY_API_KEY'):
            api_key = os.getenv('INSTANTLY_API_KEY')
        else:
            raise ValueError("API key is required. Set INSTANTLY_API_KEY in .env or pass as argument")
    
    if base_url is None:
        if os.getenv('INSTANTLY_BASE_URL'):
            base_url = os.getenv('INSTANTLY_BASE_URL')
        else:
            raise ValueError("Base URL is required. Set INSTANTLY_BASE_URL in .env or pass as argument")
    
    url = base_url + f"/api/v2/campaigns/{campaign_id}/activate"

    headers = {"Authorization": f"Bearer {api_key}"}

    response = requests.post(url, headers=headers, timeout=timeout)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error starting campaign: {response.status_code} - {response.text}")
        return {"error": f"{response.status_code} - {response.text}"}

def get_campaign(campaign_id: str, api_key: str = None, base_url: str = None, timeout: int = 60) -> dict:
    """
    Get campaign details from Instantly.ai using the campaigns API endpoint.
    
    Args:
        campaign_id (str): The campaign ID to retrieve
        api_key (str): Instantly API key
        base_url (str): Instantly API base URL
        timeout (int): Request timeout in seconds
        
    Returns:
        dict: Campaign details or error response
    """
    if api_key is None:
        if os.getenv('INSTANTLY_API_KEY'):
            api_key = os.getenv('INSTANTLY_API_KEY')
        else:
            raise ValueError("API key is required. Set INSTANTLY_API_KEY in .env or pass as argument")
    
    if base_url is None:
        if os.getenv('INSTANTLY_BASE_URL'):
            base_url = os.getenv('INSTANTLY_BASE_URL')
        else:
            raise ValueError("Base URL is required. Set INSTANTLY_BASE_URL in .env or pass as argument")
    
    url = base_url + f"/api/v2/campaigns/{campaign_id}"

    headers = {"Authorization": f"Bearer {api_key}"}

    response = requests.get(url, headers=headers, timeout=timeout)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching campaign: {response.status_code} - {response.text}")
        return {"error": f"{response.status_code} - {response.text}"}

def delete_campaign(campaign_id: str, api_key: str = None, base_url: str = None, timeout: int = 60) -> dict:
    """
    Delete a campaign in Instantly.ai using the campaigns API endpoint.
    
    Args:
        campaign_id (str): The campaign ID to delete
        api_key (str): Instantly API key
        base_url (str): Instantly API base URL
        timeout (int): Request timeout in seconds
        
    Returns:
        dict: Response from the API
    """
    if api_key is None:
        if os.getenv('INSTANTLY_API_KEY'):
            api_key = os.getenv('INSTANTLY_API_KEY')
        else:
            raise ValueError("API key is required. Set INSTANTLY_API_KEY in .env or pass as argument")
    
    if base_url is None:
        if os.getenv('INSTANTLY_BASE_URL'):
            base_url = os.getenv('INSTANTLY_BASE_URL')
        else:
            raise ValueError("Base URL is required. Set INSTANTLY_BASE_URL in .env or pass as argument")
    
    url = base_url + f"/api/v2/campaigns/{campaign_id}"

    headers = {"Authorization": f"Bearer {api_key}"}

    response = requests.delete(url, headers=headers, timeout=timeout)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error deleting campaign: {response.status_code} - {response.text}")
        return {"error": f"{response.status_code} - {response.text}"}

def create_campaign(
    country_name: str,
    api_key: str = None,
    base_url: str = None,
    timeout: int = 60,
    timezone: str = None,
    max_timezone_attempts: int = 3,
    debug: bool = False,
) -> dict:
    """
    Create a new campaign in Instantly.ai using email templates.

    Args:
        country_name (str): Campaign name, typically the country name
        api_key (str): Instantly API key
        base_url (str): Instantly API base URL
        timeout (int): Request timeout in seconds

    Returns:
        dict: API response
    """
    if not isinstance(country_name, str) or not country_name.strip():
        raise ValueError("country_name is required and must be a non-empty string")

    if api_key is None:
        if os.getenv('INSTANTLY_API_KEY'):
            api_key = os.getenv('INSTANTLY_API_KEY')
        else:
            raise ValueError("API key is required. Set INSTANTLY_API_KEY in .env or pass as argument")

    if base_url is None:
        if os.getenv('INSTANTLY_BASE_URL'):
            base_url = os.getenv('INSTANTLY_BASE_URL')
        else:
            raise ValueError("Base URL is required. Set INSTANTLY_BASE_URL in .env or pass as argument")

    max_timezone_attempts = max(1, int(max_timezone_attempts))
    timezone_prediction_context = country_name

    if timezone is not None:
        timezone = str(timezone).strip()
        if not is_valid_timezone(timezone):
            print(
                f"Provided timezone '{timezone}' is invalid. "
                "Falling back to auto-prediction."
            )
            timezone_prediction_context = (
                f"{country_name}\nPreviously rejected timezone: {timezone}"
            )
            timezone = None

    templates = email_format()
    if len(templates) < 5:
        raise ValueError("At least 5 email templates are required")

    steps = []
    for i, template in enumerate(templates):
        subject = str(get_subject_line() or "").strip()
        body = _normalize_template_body(template)

        if not subject:
            raise ValueError(f"Subject is empty for template index {i}")
        if not body:
            raise ValueError(f"Body is empty for template index {i}")

        delay = 0 if i == 0 else 1
        
        steps.append(
            {
                "type": "email",
                "delay": delay,
                "delay_unit": "days",
                "variants": [
                    {
                        "subject": subject,
                        "body": body,
                    }
                ],
            }
        )

    if debug:
        variables_found = sorted(
            {
                variable
                for step in steps
                for variable in _TEMPLATE_VARIABLE_PATTERN.findall(
                    step["variants"][0]["body"]
                )
            }
        )
        print(
            "Prepared campaign step bodies "
            f"(count={len(steps)}, first_body_len={len(steps[0]['variants'][0]['body'])})"
        )
        print(f"Template variables found in bodies: {variables_found}")

    campaign_name = country_name.strip()

    url = base_url.rstrip("/") + "/api/v2/campaigns"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    last_response = None
    campaign_payload = None

    for timezone_attempt in range(1, max_timezone_attempts + 1):
        if timezone is None:
            prediction = predict_timezone(
                context=timezone_prediction_context,
                max_retries=3,
            )
            predicted_timezone = str(prediction.get("timezone", "") or "").strip()
            is_valid_prediction = bool(prediction.get("is_valid")) and is_valid_timezone(
                predicted_timezone
            )

            print(
                f"predicted timezone attempt {timezone_attempt}: "
                f"{predicted_timezone} (valid={is_valid_prediction})"
            )

            if not is_valid_prediction:
                if predicted_timezone:
                    timezone_prediction_context = (
                        f"{country_name}\nPreviously rejected timezone: {predicted_timezone}"
                    )

                if timezone_attempt < max_timezone_attempts:
                    continue

                return {
                    "error": (
                        "Failed to predict a valid timezone after "
                        f"{max_timezone_attempts} attempts"
                    ),
                    "country_name": campaign_name,
                    "last_prediction": predicted_timezone,
                }

            timezone = predicted_timezone

        print(f"Setting timezone for {country_name}: {timezone}")

        campaign_payload = {
            "name": campaign_name,
            "campaign_schedule": {
                "schedules": [
                    {
                        "name": f"{campaign_name} Business Hours",
                        "timing": {
                            "from": "10:00",
                            "to": "19:00",
                        },
                        "days": {
                            "0": False,
                            "1": True,
                            "2": True,
                            "3": True,
                            "4": True,
                            "5": True,
                            "6": False,
                        },
                        "timezone": timezone,
                    }
                ]
            },
            "sequences": [
                {
                    "steps": steps,
                }
            ],
        }

        response = requests.post(url, headers=headers, json=campaign_payload, timeout=timeout)
        last_response = response

        if response.status_code in (200, 201):
            response_data = response.json()

            if debug:
                request_bodies = [step["variants"][0]["body"] for step in steps]
                response_bodies = _get_campaign_step_bodies(response_data)
                print(
                    "Campaign create debug: "
                    f"request_body_count={len(request_bodies)}, "
                    f"response_body_count={len(response_bodies)}"
                )

                if not response_bodies or any(not body.strip() for body in response_bodies):
                    campaign_id = response_data.get("id")
                    if campaign_id:
                        campaign_details = get_campaign(
                            campaign_id=campaign_id,
                            api_key=api_key,
                            base_url=base_url,
                            timeout=timeout,
                        )
                        server_bodies = _get_campaign_step_bodies(campaign_details)
                        print(
                            "Campaign details debug: "
                            f"campaign_id={campaign_id}, "
                            f"server_body_count={len(server_bodies)}, "
                            f"empty_server_bodies={sum(1 for body in server_bodies if not body.strip())}"
                        )

            return response_data

        if _is_timezone_validation_error(response.status_code, response.text):
            print(
                f"Timezone '{timezone}' rejected by Instantly "
                f"(attempt {timezone_attempt}/{max_timezone_attempts})."
            )

            if timezone:
                timezone_prediction_context = (
                    f"{country_name}\nPreviously rejected timezone: {timezone}"
                )
            timezone = None

            if timezone_attempt < max_timezone_attempts:
                print("Retrying with a fresh timezone prediction...")
                continue

        print(f"Error creating campaign: {response.status_code} - {response.text}")
        return {
            "error": f"{response.status_code} - {response.text}",
            "request_payload": campaign_payload,
        }

    return {
        "error": "Failed to create campaign after timezone retry attempts",
        "request_payload": campaign_payload,
        "last_response": (
            None
            if last_response is None
            else f"{last_response.status_code} - {last_response.text}"
        ),
    }