import pandas as pd
import requests
from io import BytesIO
from urllib.parse import unquote, urlparse
from msal import ConfidentialClientApplication


def _get_graph_access_token(client_id, client_secret, tenant_id):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    token = app.acquire_token_for_client(scopes=scopes)

    access_token = token.get("access_token")
    if not access_token:
        raise Exception(f"Unable to get Graph access token: {token}")

    return access_token


def _extract_site_path_and_drive_path(file_url):
    parsed = urlparse(file_url)
    path = unquote(parsed.path)

    if "/Documents/" in path:
        site_path = path.split("/Documents/")[0]
        drive_path = f"Documents/{path.split('/Documents/', 1)[1]}"
    else:
        site_path = "/".join(path.split("/")[:-1])
        drive_path = path.split("/")[-1]

    return parsed.netloc, site_path, drive_path


def _get_site_id(file_url, headers):
    host, site_path, _ = _extract_site_path_and_drive_path(file_url)
    site_url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site_path}:"
    site_response = requests.get(site_url, headers=headers, timeout=30)

    if site_response.status_code != 200:
        raise Exception(f"Error getting site: {site_response.text}")

    return site_response.json()["id"]


def _get_drive_path(file_url):
    _, _, drive_path = _extract_site_path_and_drive_path(file_url)
    return drive_path


def _get_drive_path_candidates(file_url):
    parsed = urlparse(file_url)
    path = unquote(parsed.path)
    is_personal_site = path.startswith("/personal/")

    candidates = []
    primary = _get_drive_path(file_url)
    basename = path.split("/")[-1]

    # Personal OneDrive URLs commonly contain /Documents/ in the browser URL,
    # while Graph drive paths often resolve from root using basename.
    ordered = [basename, primary] if is_personal_site else [primary, basename]
    for candidate in ordered:
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    if "/Documents/" in path:
        docs_relative = f"Documents/{path.split('/Documents/', 1)[1]}"
        if docs_relative not in candidates:
            candidates.append(docs_relative)

    return candidates


def get_sharepoint_file(file_url, client_id, client_secret, tenant_id, sheet_name=None):
    """Download an Excel file from SharePoint and return it as a DataFrame."""
    access_token = _get_graph_access_token(client_id, client_secret, tenant_id)
    headers = {"Authorization": f"Bearer {access_token}"}

    site_id = _get_site_id(file_url, headers)
    last_error = None
    tried = []

    for drive_path in _get_drive_path_candidates(file_url):
        file_api_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{drive_path}:/content"
        tried.append(drive_path)
        file_response = requests.get(file_api_url, headers=headers, timeout=60)

        if file_response.status_code == 200:
            return pd.read_excel(BytesIO(file_response.content), engine="openpyxl", sheet_name=sheet_name)

        last_error = file_response.text

    raise Exception(
        f"Error getting file. Tried drive paths={tried}. Last response={last_error}"
    )


def _download_sharepoint_file_bytes(file_url, client_id, client_secret, tenant_id):
    access_token = _get_graph_access_token(client_id, client_secret, tenant_id)
    headers = {"Authorization": f"Bearer {access_token}"}

    site_id = _get_site_id(file_url, headers)

    for drive_path in _get_drive_path_candidates(file_url):
        file_api_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{drive_path}:/content"
        response = requests.get(file_api_url, headers=headers, timeout=60)
        if response.status_code == 200:
            return response.content
        if response.status_code == 404:
            continue
        raise Exception(f"Error downloading file bytes: {response.text}")

    return None


def upload_excel_sheets_to_sharepoint(file_url, sheet_frames, client_id, client_secret, tenant_id):
    """Upsert one or more sheets in a SharePoint Excel workbook while preserving existing sheets."""
    existing_content = _download_sharepoint_file_bytes(file_url, client_id, client_secret, tenant_id)
    existing_sheets = {}

    if existing_content:
        with pd.ExcelFile(BytesIO(existing_content), engine="openpyxl") as xls:
            for name in xls.sheet_names:
                existing_sheets[name] = xls.parse(name)

    for name, frame in sheet_frames.items():
        existing_sheets[name] = frame

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for name, frame in existing_sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    buffer.seek(0)

    access_token = _get_graph_access_token(client_id, client_secret, tenant_id)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    site_id = _get_site_id(file_url, headers)

    last_error = None
    tried = []
    for drive_path in _get_drive_path_candidates(file_url):
        upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{drive_path}:/content"
        tried.append(drive_path)
        response = requests.put(upload_url, headers=headers, data=buffer.getvalue(), timeout=120)

        if response.status_code in (200, 201):
            payload = response.json()
            payload["_resolved_drive_path"] = drive_path
            payload["_sheet_names"] = list(existing_sheets.keys())
            return payload

        last_error = response.text

    raise Exception(
        f"Error uploading workbook sheets. Tried drive paths={tried}. Last response={last_error}"
    )


def upload_df_to_sharepoint_excel(df, file_url, client_id, client_secret, tenant_id):
    """Upload a DataFrame as an Excel file to SharePoint using Microsoft Graph."""
    access_token = _get_graph_access_token(client_id, client_secret, tenant_id)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    site_id = _get_site_id(file_url, headers)

    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    last_error = None
    tried = []

    for drive_path in _get_drive_path_candidates(file_url):
        upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{drive_path}:/content"
        tried.append(drive_path)
        response = requests.put(upload_url, headers=headers, data=buffer.getvalue(), timeout=120)

        if response.status_code in (200, 201):
            payload = response.json()
            payload["_resolved_drive_path"] = drive_path
            return payload

        last_error = response.text

    raise Exception(
        f"Error uploading file. Tried drive paths={tried}. Last response={last_error}"
    )
