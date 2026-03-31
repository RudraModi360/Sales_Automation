import pandas as pd
import requests
from io import BytesIO
from msal import ConfidentialClientApplication


def get_sharepoint_file(file_url, client_id, client_secret, tenant_id, sheet_name=None):
    """
    Download file from SharePoint OneDrive using app-only authentication.
    
    Args:
        file_url: Full SharePoint URL of the file (e.g., https://tecblic1-my.sharepoint.com/personal/...)
        client_id: Azure AD app client ID
        client_secret: Azure AD app client secret
        tenant_id: Azure AD tenant ID
        sheet_name: Sheet name to read (optional, default None reads first sheet)
    
    Returns:
        pandas.DataFrame: The parsed Excel file content
    """
    # Get access token
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]
    
    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    token = app.acquire_token_for_client(scopes=scope)
    access_token = token['access_token']
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Get site using hostname and path
    site_url = "https://graph.microsoft.com/v1.0/sites/tecblic1-my.sharepoint.com:/personal/rudra_modi_tecblic_com:"
    site_response = requests.get(site_url, headers=headers)
    
    if site_response.status_code != 200:
        raise Exception(f"Error getting site: {site_response.text}")
    
    site_id = site_response.json()['id']
    
    # Extract filename from file_url
    filename = file_url.split('/')[-1]
    
    # Get file content using the site drive
    file_api_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{filename}:/content"
    file_response = requests.get(file_api_url, headers=headers)
    
    if file_response.status_code != 200:
        raise Exception(f"Error getting file: {file_response.text}")
    
    # Parse and return as DataFrame
    df = pd.read_excel(BytesIO(file_response.content), engine='openpyxl', sheet_name=sheet_name)
    return df
