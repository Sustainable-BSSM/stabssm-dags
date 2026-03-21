import json
import logging
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
ROOT_FOLDER_ID = "1N-Av8f7jb5Fm8i_kspSwnpomIknTMaOZ"


def _build_service():
    info = json.loads(os.environ["GOOGLE_OAUTH_TOKEN_JSON"])
    creds = Credentials.from_authorized_user_info(info, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _get_or_create_folder(service, name: str, parent_id: str) -> str:
    query = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder'"
        f" and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(
        q=query,
        fields="files(id)",
        spaces="drive",
    ).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    folder = service.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
    ).execute()
    return folder["id"]


def upload_newsletter(pdf_path: str, year: str, month: str) -> str:
    """PDF를 ROOT/{year}/{month}/ 에 업로드하고 file id 반환."""
    service = _build_service()

    year_folder_id = _get_or_create_folder(service, year, ROOT_FOLDER_ID)
    month_folder_id = _get_or_create_folder(service, month, year_folder_id)

    file_name = Path(pdf_path).name
    media = MediaFileUpload(pdf_path, mimetype="application/pdf", resumable=True)
    uploaded = service.files().create(
        body={"name": file_name, "parents": [month_folder_id]},
        media_body=media,
        fields="id",
    ).execute()

    file_id = uploaded["id"]
    logger.info(f"[GDriveUploader] 업로드 완료: {file_name} → {year}/{month}/ (id={file_id})")
    return file_id
