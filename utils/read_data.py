import pandas as pd
import requests
import base64
import time
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


def normalize_sharepoint_file_url(file_url):
    """Validate and normalize to canonical direct SharePoint/OneDrive file URLs."""
    parsed = urlparse(file_url)
    path = unquote(parsed.path)

    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Invalid SharePoint URL provided.")

    if "/_layouts/15/Doc.aspx" in path or path.startswith("/:"):
        raise ValueError(
            "SharePoint Doc.aspx links are not supported. Use direct file URLs like "
            "https://<tenant>-my.sharepoint.com/personal/<user>/Documents/<file>.xlsx"
        )

    if "/Documents/" not in path:
        raise ValueError(
            "Only direct OneDrive/SharePoint file URLs under /Documents/ are supported. "
            f"Received path: {path}"
        )

    return file_url


def _get_site_id(file_url, headers):
    normalize_sharepoint_file_url(file_url)
    host, site_path, _ = _extract_site_path_and_drive_path(file_url)
    site_url = f"https://graph.microsoft.com/v1.0/sites/{host}:{site_path}:"
    site_response = requests.get(site_url, headers=headers, timeout=30)

    if site_response.status_code != 200:
        raise Exception(f"Error getting site: {site_response.text}")

    return site_response.json()["id"]


def _get_drive_path(file_url):
    normalize_sharepoint_file_url(file_url)
    _, _, drive_path = _extract_site_path_and_drive_path(file_url)
    return drive_path


def _get_drive_path_candidates(file_url):
    # Keep existing callsites stable while enforcing deterministic single-path behavior.
    return [_get_drive_path(file_url)]


def _get_download_drive_paths(file_url):
    """Return ordered read paths: canonical first, then safe basename fallback."""
    primary = _get_drive_path(file_url)
    parsed = urlparse(file_url)
    basename = unquote(parsed.path).split("/")[-1]

    paths = [primary]
    if basename and basename != primary:
        paths.append(basename)

    return paths


def _is_share_link(file_url):
    parsed = urlparse(file_url)
    path = unquote(parsed.path)
    return "/_layouts/15/Doc.aspx" in path or path.startswith("/:")


def _to_graph_share_id(file_url):
    token = base64.b64encode(file_url.encode("utf-8")).decode("utf-8")
    token = token.rstrip("=").replace("/", "_").replace("+", "-")
    return f"u!{token}"


def _download_via_share_link(file_url, headers):
    share_id = _to_graph_share_id(file_url)
    share_api_url = f"https://graph.microsoft.com/v1.0/shares/{share_id}/driveItem/content"
    response = requests.get(share_api_url, headers=headers, timeout=60)
    if response.status_code == 200:
        return response.content
    return None


def _is_resource_locked_response(response):
    if response.status_code in (423, 409):
        return True

    try:
        payload = response.json()
    except Exception:
        return False

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    inner = error.get("innerError", {}) if isinstance(error, dict) else {}
    message = (error.get("message") or "").lower()
    inner_code = (inner.get("code") or "").lower()

    return inner_code == "resourcelocked" or "locked" in message


def _put_with_lock_retry(upload_url, headers, data, timeout, retries=5, delay_seconds=2):
    last_response = None
    for attempt in range(retries + 1):
        response = requests.put(upload_url, headers=headers, data=data, timeout=timeout)
        last_response = response

        if response.status_code in (200, 201):
            return response

        if _is_resource_locked_response(response) and attempt < retries:
            time.sleep(delay_seconds)
            continue

        return response

    return last_response


def get_sharepoint_file(file_url, client_id, client_secret, tenant_id, sheet_name=None):
    """Download an Excel file from SharePoint and return it as a DataFrame."""
    file_url = normalize_sharepoint_file_url(file_url)
    content = _download_sharepoint_file_bytes(file_url, client_id, client_secret, tenant_id)
    if not content:
        raise Exception("Error getting file: no content returned from SharePoint.")
    return pd.read_excel(BytesIO(content), engine="openpyxl", sheet_name=sheet_name)


def _download_sharepoint_file_bytes(file_url, client_id, client_secret, tenant_id):
    file_url = normalize_sharepoint_file_url(file_url)
    access_token = _get_graph_access_token(client_id, client_secret, tenant_id)
    headers = {"Authorization": f"Bearer {access_token}"}

    site_id = _get_site_id(file_url, headers)

    for drive_path in _get_download_drive_paths(file_url):
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
    file_url = normalize_sharepoint_file_url(file_url)
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
    drive_path = _get_drive_path(file_url)
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{drive_path}:/content"
    response = _put_with_lock_retry(
        upload_url,
        headers=headers,
        data=buffer.getvalue(),
        timeout=120,
    )

    if response.status_code in (200, 201):
        payload = response.json()
        payload["_resolved_drive_path"] = drive_path
        payload["_sheet_names"] = list(existing_sheets.keys())
        payload["_upload_status_code"] = response.status_code
        payload["_upload_mode"] = "updated" if response.status_code == 200 else "created"
        payload["_created_new_file"] = response.status_code == 201
        return payload

    raise Exception(
        "Error uploading workbook sheets. "
        f"Drive path={drive_path}. Response={response.text}"
    )


def upload_df_to_sharepoint_excel(df, file_url, client_id, client_secret, tenant_id):
    """Upload a DataFrame as an Excel file to SharePoint using Microsoft Graph."""
    file_url = normalize_sharepoint_file_url(file_url)
    access_token = _get_graph_access_token(client_id, client_secret, tenant_id)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    site_id = _get_site_id(file_url, headers)

    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    drive_path = _get_drive_path(file_url)
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{drive_path}:/content"
    response = _put_with_lock_retry(
        upload_url,
        headers=headers,
        data=buffer.getvalue(),
        timeout=120,
    )

    if response.status_code in (200, 201):
        payload = response.json()
        payload["_resolved_drive_path"] = drive_path
        payload["_upload_status_code"] = response.status_code
        payload["_upload_mode"] = "updated" if response.status_code == 200 else "created"
        payload["_created_new_file"] = response.status_code == 201
        return payload

    raise Exception(
        "Error uploading file. "
        f"Drive path={drive_path}. Response={response.text}"
    )
