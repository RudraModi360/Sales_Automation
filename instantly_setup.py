import os
import time
from instantly.campaigns import list_campaigns,create_campaign
from instantly.leads import upload_series_lead_to_campaign
from instantly.data_inputs import external_schema_converter, data_read
from instantly.grouping import existing_country_campaigns
from instantly.timezone_prediction import predict_timezone
from instantly.email_generation import  email_chain_generation, person_data_explorer,_build_anthropic_client
from instantly.helper import restructure_response
import pandas as pd
from groq import Groq

client = _build_anthropic_client()

client_groq = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

def main(max_retries: int = 3):
    df = external_schema_converter(
        data_read(
            file_url="US NY - Financial Services - 10 mn to 500 mn - Copy.csv",
            sheet_name="in",
        )
    )

    campaigns_response = list_campaigns(
        api_key=os.getenv("INSTANTLY_API_KEY"),
        base_url=os.getenv("INSTANTLY_BASE_URL"),
    )

    non_matching_country_groups = existing_country_campaigns(campaigns_response, df)
    final_series = pd.Series(dtype=object)
    total_uploaded_records = 0
    total_upload_seconds = 0.0
    
    for country in non_matching_country_groups:
        for key , val in country.items():
            campaign_response = create_campaign(country_name=key)
            if campaign_response.get("error"):
                print(f"Campaign creation failed for {key}: {campaign_response['error']}")
                continue

            campaign_id = campaign_response.get("id")
            if not campaign_id:
                print(f"Campaign id missing for {key}: {campaign_response}")
                continue

            print(campaign_id)
            for i,record in enumerate(val):
                if i==5:
                    break
                record=pd.Series(record)
                result=email_chain_generation(client=client,df=record,person_context=person_data_explorer(client_groq, record))
                result=restructure_response(result)
                final_series = pd.concat([record, result])
                print(final_series)

                upload_start = time.perf_counter()
                upload_response = upload_series_lead_to_campaign(
                    lead_series=final_series,
                    campaign_id=campaign_id,
                    api_key=os.getenv("INSTANTLY_API_KEY"),
                    base_url=os.getenv("INSTANTLY_BASE_URL"),
                    skip_if_in_workspace=True,
                    verify_leads_on_import=False,
                )
                record_upload_seconds = time.perf_counter() - upload_start
                total_uploaded_records += 1
                total_upload_seconds += record_upload_seconds

                print(
                    f"Record : {i} processed and uploaded to campaign {campaign_id} "
                    f"in {record_upload_seconds:.2f} seconds"
                )

    if total_uploaded_records:
        print(
            f"Total upload time for all records: {total_upload_seconds:.2f} seconds "
            f"for {total_uploaded_records} records"
        )
        print(
            f"Average upload time per record: "
            f"{(total_upload_seconds / total_uploaded_records):.2f} seconds"
        )
    else:
        print("No records were uploaded.")

if __name__ == "__main__":
    main()