import pandas as pd
import requests
from io import BytesIO
import os
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()
def read_df(file_url, client_id, client_secret, tenant_id, sheet_name=None):
    """
    Download file from SharePoint OneDrive using app-only authentication.
    
    Args:
        file_url: File name under /Documents/Documents in OneDrive (e.g., 'Sales_Results.xlsx')
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


def write_df(df, file_name=None, sheet_name='Data'):
    """
    Write or append DataFrame to local Excel file.
    If file doesn't exist, create it. If exists, append records to it.
    
    Args:
        df (pd.DataFrame): DataFrame to write
        file_name (str): Output file name. If None, reads from env var OUTPUT_FILE_NAME
        sheet_name (str): Sheet name in Excel file (default: 'Data')
    
    Returns:
        str: Path to the saved/updated file
    """
    if file_name is None:
        file_name = os.getenv('OUTPUT_FILE_NAME')
    
    if not file_name:
        raise ValueError("OUTPUT_FILE_NAME environment variable not set and file_name not provided")
    
    # Normalize file path
    file_path = os.path.abspath(file_name)
    
    try:
        if os.path.exists(file_path):
            # File exists - append records
            existing_df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_excel(file_path, sheet_name=sheet_name, index=False, engine='openpyxl')
            print(f"✓ Appended {len(df)} records to {file_path} (Total: {len(combined_df)} rows)")
        else:
            # File doesn't exist - create new file
            df.to_excel(file_path, sheet_name=sheet_name, index=False, engine='openpyxl')
            print(f"✓ Created new file {file_path} with {len(df)} records")
        
        return file_path
    
    except Exception as e:
        print(f"✗ Error writing to {file_path}: {str(e)}")
        raise


def write_df_remote(df, file_name, client_id, client_secret, tenant_id, sheet_name='Data'):
    """
    Write or append DataFrame to remote Excel file on SharePoint OneDrive.
    If file exists, appends records. If not, creates new file.
    
    Args:
        df (pd.DataFrame): DataFrame to write
        file_name (str): File name only (e.g., 'Sales_Results.xlsx')
        client_id (str): Azure AD app client ID
        client_secret (str): Azure AD app client secret
        tenant_id (str): Azure AD tenant ID
        sheet_name (str): Sheet name in Excel file (default: 'Data')
    
    Returns:
        str: File URL if successful
    """
    from io import BytesIO
    
    # Construct full file URL from base path
    base_url = "https://tecblic1-my.sharepoint.com/personal/rudra_modi_tecblic_com/Documents"
    file_url = f"{base_url}/{file_name}"
    
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
    
    # Get file API endpoint
    file_api_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/Documents/{file_name}:/content"
    print(f"[DEBUG] File API URL: {file_api_url}")
    print(f"[DEBUG] Target OneDrive URL: {file_url}")
    
    try:
        # Try to read existing file
        file_response = requests.get(file_api_url, headers=headers)
        
        if file_response.status_code == 200:
            # File exists - append records
            existing_df = pd.read_excel(BytesIO(file_response.content), engine='openpyxl', sheet_name=sheet_name)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            final_df = combined_df
            action = "Appended"
        else:
            # File doesn't exist - use new data
            final_df = df
            action = "Created"
        
        # Convert DataFrame to Excel bytes
        excel_buffer = BytesIO()
        final_df.to_excel(excel_buffer, sheet_name=sheet_name, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        
        # Upload to SharePoint
        upload_response = requests.put(
            file_api_url,
            data=excel_buffer.getvalue(),
            headers=headers
        )        
        if upload_response.status_code not in [200, 201]:
            raise Exception(f"Error uploading file (Status {upload_response.status_code}): {upload_response.text}")
        
        print(f"✓ {action} {len(df)} records to remote file (Total: {len(final_df)} rows)")
        print(f"✓ File accessible at: {file_url}")
        return file_url
    
    except Exception as e:
        print(f"✗ Error writing to remote file {file_url}: {str(e)}")
        raise


