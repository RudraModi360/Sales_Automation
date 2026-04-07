import pandas as pd
import os
from instantly.mapping import transform_overview_to_summary, external_to_overview_df

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import read_df
from dotenv import load_dotenv

load_dotenv()

def data_read(
    file_url: str = None,
    sheet_name: str = None
)->pd.DataFrame:
    """
    Read Excel file from OneDrive using Microsoft Graph API and return as DataFrame.
    
    Args:
        file_url (str): URL of the Excel file on SharePoint
        sheet_name (str): Sheet name in the Excel file
    """
    if file_url is None:
        if os.getenv('OUTPUT_FILE_NAME') is not None:
            file_url = os.getenv('OUTPUT_FILE_NAME')
        else:
            raise ValueError("file_url must be provided")
        
    if sheet_name is None:
        sheet_name = "summary_people"
    
    try:
        df = read_df(file_url=file_url, sheet_name=sheet_name)
        print(f"[OK] Successfully read data from {file_url}")
        return df
    except Exception as e:
        print(f"[ERROR] Error reading data from {file_url}: {str(e)}")
        raise

def external_schema_converter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert external data schema to internal summary schema.
    
    Args:
        df (pd.DataFrame): Input DataFrame with external schema 
    Returns:
        pd.DataFrame: DataFrame with internal summary schema
    """
    if df is None:
        raise ValueError("Input DataFrame cannot be None")

    overview_df = external_to_overview_df(df)
    summary_df = transform_overview_to_summary(overview_df)
    return summary_df
