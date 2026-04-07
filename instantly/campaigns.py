import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


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
    