import pandas as pd
import os

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import read_df

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
            file_url = "https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents/Documents/"+os.getenv('OUTPUT_FILE_NAME')
        else:
            raise ValueError("file_url must be provided")
        
    if sheet_name is None:
        sheet_name = "summary_people"
    
    print("File URL : ",file_url)
    print("Sheet Name : ",sheet_name)
    try:
        df = read_df(file_url=file_url, sheet_name=sheet_name)
        print(f"✓ Successfully read data from {file_url}")
        return df
    except Exception as e:
        print(f"✗ Error reading data from {file_url}: {str(e)}")
        raise
    

# Test the function
if __name__ == "__main__":
    try:
        df = data_read()
        print(f"✓ Data loaded successfully with shape {df.shape}")
    except Exception as e:
        print(f"Note: Could not load data from SharePoint. Error: {str(e)[:100]}...")
        print("To use this function, provide a valid file_url or ensure OUTPUT_FILE_NAME is set to an existing SharePoint file.")