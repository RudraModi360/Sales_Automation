import pandas as pd
import requests
from io import BytesIO
from urllib.parse import urlparse, unquote
import os
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

def get_access_token(client_id, client_secret, tenant_id):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    token = app.acquire_token_for_client(scopes=scope)

    if "access_token" not in token:
        raise Exception(f"Token error: {token}")

    return token["access_token"]


def extract_onedrive_info(file_url):
    parsed = urlparse(file_url)
    path = unquote(parsed.path)

    if "/personal/" not in path:
        raise ValueError("Only personal OneDrive URLs supported in this function")

    part = path.split("/personal/")[1]
    file_path = "/".join(part.split("/")[1:])

    if not file_path:
        raise ValueError(f"Could not extract file path from URL: {file_url}")

    user_email = "rudra.modi@tecblic.com"
    return user_email, file_path


def search_file(headers, user_email, file_name):
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/root/search(q='{file_name}')"
    res = requests.get(url, headers=headers)

    if res.status_code != 200:
        raise Exception(f"Search failed: {res.text}")

    results = res.json().get("value", [])
    if not results:
        raise Exception("File not found via search")

    for item in results:
        if item["name"] == file_name:
            return item["id"]

    return results[0]["id"]


def read_df(file_url : str, client_id = None, client_secret =None , tenant_id=None, sheet_name=None):
    """
    Download CSV/Excel file from personal OneDrive via Microsoft Graph.

    Args:
        file_url: Full OneDrive URL or file name.
        client_id: Azure AD app client ID.
        client_secret: Azure AD app client secret.
        tenant_id: Azure AD tenant ID.
        sheet_name: Optional sheet name for Excel files.

    Returns:
        pandas.DataFrame (or dict of DataFrames if sheet_name=None for Excel).
    """
    if client_id is None:
        if os.getenv('CLIENT_ID'):
            client_id = os.getenv('CLIENT_ID')
        else:
            raise ValueError("Azure client ID is required. Set CLIENT_ID in .env or pass as argument ")
    
    if client_secret is None:
        if os.getenv('CLIENT_SECRETS'):
            client_secret = os.getenv('CLIENT_SECRETS')
        else:
            raise ValueError("Azure client secret is required. Set CLIENT_SECRET in .env or pass as argument")
    
    if tenant_id is None:
        if os.getenv('TENANT_ID'):
            tenant_id = os.getenv('TENANT_ID')
        else:
            raise ValueError("Azure tenant ID is required. Set TENANT_ID in .env or pass as argument")
        
    if sheet_name is None:
        if os.getenv('SHEET_NAME'):
            sheet_name = os.getenv('CONFIG_SHEET_NAME')
        else:
            raise ValueError("Sheet name is required for Excel files. Set SHEET_NAME in .env or pass as argument")
    access_token = get_access_token(client_id, client_secret, tenant_id)
    headers = {"Authorization": f"Bearer {access_token}"}

    user_email = "rudra.modi@tecblic.com"

    if file_url.startswith("http"):
        user_email, file_path = extract_onedrive_info(file_url)
    else:
        file_path = f"Documents/{file_url}"

    graph_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/root:/{file_path}:/content"

    response = requests.get(graph_url, headers=headers)

    if response.status_code != 200:
        file_name = file_path.split("/")[-1]
        file_id = search_file(headers, user_email, file_name)
        graph_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/items/{file_id}/content"

        response = requests.get(graph_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching file: {response.text}")

    content = BytesIO(response.content)
    file_name_lower = file_path.lower()

    if file_name_lower.endswith(".csv"):
        return pd.read_csv(content)

    if file_name_lower.endswith(".xlsx") or file_name_lower.endswith(".xls"):
        return pd.read_excel(content, engine="openpyxl", sheet_name=sheet_name)

    raise ValueError("Only CSV and Excel files supported")


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

