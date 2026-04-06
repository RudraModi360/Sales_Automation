import pandas as pd
from utils import read_df, write_df_remote
from apollo.basic_processing import build_raw_df, build_overview_df
from apollo.summary import transform_overview_to_summary


df=read_df("https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/apollo_people_match_overview.xlsx",sheet_name="Sheet1")

raw_df=build_raw_df(df.to_dict(orient='records'))

overview_df=build_overview_df(raw_df)

summary_df=transform_overview_to_summary(overview_df)

try:
    write_df_remote(df=summary_df,sheet_name="summary_people_df")
    print("Summary Data successfully written to remote Excel file.")
except Exception as e:
    print(f"Error writing to remote Excel file: {e}")