from apollo.filters import filter_data
from apollo.search_people import search_apollo_people
from utils import read_df,write_df_remote
from apollo.basic_processing import build_raw_df, build_overview_df
from apollo.enrich_people import get_people_enrich
from apollo.summary import transform_overview_to_summary
import os
from dotenv import load_dotenv 

load_dotenv()

def main():
    # Step 1: Read filters from Excel
    df = read_df()

    # Step 2: Process filters into API format
    filters = filter_data(df)
    
    # Step 3: Search Apollo People API with filters
    results = search_apollo_people(filters,api_key=os.getenv('APOLLO_API_KEY'))
    
    print("Total Records Found:", results.get("total_records"))
    
    results=get_people_enrich(
        api_key=os.getenv('APOLLO_API_KEY'),
        results=results)
    
    raw_df = build_raw_df(results)
    overview_df = build_overview_df(results)
    summary_df = transform_overview_to_summary(overview_df)
    try:
        write_df_remote(df=raw_df,sheet_name="raw_data")
        print("Raw Data successfully written to remote Excel file.")
        write_df_remote(df=overview_df,sheet_name="overview_people")
        print("Overview Data successfully written to remote Excel file.")
        write_df_remote(df=summary_df,sheet_name="summary_people")
        print("Summary Data successfully written to remote Excel file.")
    except Exception as e:
        print(f"Error writing to remote Excel file: {e}")

if __name__ == "__main__":
    main()