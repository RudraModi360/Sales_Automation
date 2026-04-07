import os

from instantly.campaigns import list_campaigns
from instantly.data_inputs import external_schema_converter, data_read
from instantly.grouping import existing_country_campaigns
from instantly.timezone_prediction import predict_timezone


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
    non_matching_country_names = [
        next(iter(country_group.keys()))
        for country_group in non_matching_country_groups
        if country_group
    ]

    print("Non-matching countries:", non_matching_country_names)
    # print("Non-matching country data:", non_matching_country_groups)
    
    timezones=[]
    for country in non_matching_country_names:
        for i in range(max_retries):  # Try up to 3 times for each country
            try:
                timezone=predict_timezone(context=country,max_retries=max_retries)
                timezones.append({"country": country, "predicted_timezone": timezone})
                print(f"Predicted timezone for {country}: {timezone}")
                break
            except Exception as e:
                print(f"Error predicting timezone for {country}: {str(e)}")
                timezone="Unknown"
    print("Final predicted timezones:", timezones)
    
if __name__ == "__main__":
    main()