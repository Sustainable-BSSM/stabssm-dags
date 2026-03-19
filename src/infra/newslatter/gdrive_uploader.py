import json
import logging
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
ROOT_FOLDER_ID = "1wsmmHkb0kP2RY3ab9GBD4I5mU1AdT0uI"


def _build_service():
    raw = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _get_or_create_folder(service, name: str, parent_id: str) -> str:
    query = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder'"
        f" and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


def upload_newsletter(pdf_path: str, year: str, month: str) -> str:
    """PDF를 ROOT/{year}/{month}/ 에 업로드하고 file id 반환."""
    service = _build_service()

    year_folder_id = _get_or_create_folder(service, year, ROOT_FOLDER_ID)
    month_folder_id = _get_or_create_folder(service, month, year_folder_id)

    file_name = Path(pdf_path).name
    media = MediaFileUpload(pdf_path, mimetype="application/pdf", resumable=True)
    file_meta = {"name": file_name, "parents": [month_folder_id]}
    uploaded = service.files().create(body=file_meta, media_body=media, fields="id").execute()

    file_id = uploaded["id"]
    logger.info(f"[GDriveUploader] 업로드 완료: {file_name} → {year}/{month}/ (id={file_id})")
    return file_id
