import requests
import pandas as pd
import os

def search_apollo_people(
    filters: dict,
    api_key: None | str,
    batch_interval: int = 2000,
    batch_size: int = 5,
    max_pages: int = 2,
    timeout: int = 30
) -> dict:
    """
    Search for people in Apollo.io using mixed_people API endpoint.
    
    Args:
        filters (dict): Filter parameters for the search
        api_key (str): Apollo API key
        batch_size (int): Number of records per batch (default: 5)
        batch_interval (int): Interval between batches in milliseconds (default: 2000)
        max_pages (int): Maximum number of pages to fetch (default: 20)
        timeout (int): Request timeout in seconds (default: 30)
    
    Returns:
        dict: Combined results from all pages
    """
    import time
    
    if api_key is None:
        if os.getenv('APOLLO_API_KEY'):
            api_key = os.getenv('APOLLO_API_KEY')
        else:
            raise ValueError("API key is required. Set APOLLO_API_KEY in .env or pass as argument.")
    url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    
    headers = {
        "Cache-Control": "no-cache",
        "accept": "application/json",
        "x-api-key": api_key
    }
    
    all_results = {
        "people": [],
        "meta": {},
        "page_count": 0
    }
    
    try:
        for page in range(1, max_pages + 1):
            # Add pagination parameter
            payload = filters.copy()
            payload["page"] = page
            
            print(f"Fetching page {page}...")
            
            response = requests.post(
                url,
                params=payload,
                headers=headers,
                timeout=timeout
            )
            
            
            response.raise_for_status()
            data = response.json()
            
            if "people" in data:
                all_results["people"].extend(data["people"])
            
            if "meta" in data:
                all_results["meta"] = data["meta"]
                all_results["page_count"] = page
                
                # Check if we've reached the last page
                if data["meta"].get("pagination", {}).get("total_pages", 1) == page:
                    print(f"Reached last page: {page}")
                    break
            
            # Wait between batches (convert milliseconds to seconds)
            if page < max_pages:
                time.sleep(batch_interval / 1000)
        
        all_results['total_records']=data['total_entries']
        return all_results
    
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return {"error": str(e), "people": [], "meta": {}}
