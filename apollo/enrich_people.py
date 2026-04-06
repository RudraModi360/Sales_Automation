import requests
import pandas as pd
import os
import sys
from basic_processing import build_raw_df, build_overview_df
# Add parent directory to path for utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import write_df

def get_people_enrich(
    api_key : None| str,
    results : dict,
    timeout : int = 300,
    max_iterations : int = None,
    output_file_name : str = "apollo_enriched_people.xlsx",
):
    # Load max_iterations from env or use default
    if max_iterations is None:
        max_iterations = int(os.getenv('MAX_ENRICH_ITERATIONS', 1000))
    url = "https://api.apollo.io/api/v1/people/match"
    headers = {
        "Cache-Control": "no-cache",
        "accept": "application/json",
        "x-api-key": api_key
    }

    success_count = 0
    failed_count = 0
    raw_rows = []

    for i,people in enumerate(results.get('people', [])):
        if i >= max_iterations:
            print(f"Reached max iterations limit: {max_iterations}")
            break   
        
        # Use minimal required fields: Apollo ID is the unique identifier
        required = {
            "id": people.get('id'),
            "run_waterfall_email": "false",
            "run_waterfall_phone": "false",
            "reveal_personal_emails": "true",
            "reveal_phone_number": "false"
        }
        
        try:
            response = requests.post(
                url,
                params=required,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            data = response.json()

            flat = pd.json_normalize(data, sep='.')
            if not flat.empty:
                raw_rows.append(flat.iloc[0].to_dict())
                success_count += 1
                print(f"Collected {success_count} enrichment rows")
            else:
                failed_count += 1
                print(f"Empty response for id={required['id']}")

        except requests.exceptions.RequestException as e:
            failed_count += 1
            # Print detailed error info for debugging
            print(f"Failed for id={required['id']}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_body = e.response.json()
                    print(f"  Error details: {error_body}")
                except:
                    print(f"  Response text: {e.response.text[:500]}")

    # Convert raw_rows to DataFrame and save locally
    if raw_rows:
        raw_df = pd.DataFrame(raw_rows)
        write_df(raw_df, file_name=output_file_name, sheet_name='Enriched_Data')
        print(f"✓ Saved {len(raw_df)} enriched records to {output_file_name}")
        build_raw_df(raw_rows).to_excel(f"raw_{output_file_name}", index=False)
        print(f"✓ Saved raw data to raw_{output_file_name}")
        build_overview_df(raw_df).to_excel(f"overview_{output_file_name}", index=False)
        print(f"✓ Saved overview to overview_{output_file_name}")
    else:
        print("No enrichment records to save.")

    print(f"Enrichment complete. Success={success_count}, Failed={failed_count}")
    return raw_rows