import json
import logging
import os
import ssl
from pathlib import Path

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from urllib3.util.ssl_ import create_urllib3_context

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
ROOT_FOLDER_ID = "1N-Av8f7jb5Fm8i_kspSwnpomIknTMaOZ"


class _TLSAdapter(requests.adapters.HTTPAdapter):
    """Python 3.12 SSL UNEXPECTED_EOF_WHILE_READING 우회 어댑터."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        if hasattr(ssl, "OP_IGNORE_UNEXPECTED_EOF"):
            ctx.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def _build_service():
    info = json.loads(os.environ["GOOGLE_OAUTH_TOKEN_JSON"])
    creds = Credentials.from_authorized_user_info(info, SCOPES)
    if creds.expired and creds.refresh_token:
        session = requests.Session()
        session.mount("https://", _TLSAdapter())
        creds.refresh(Request(session=session))
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


def _find_file(service, name: str, parent_id: str) -> str | None:
    results = service.files().list(
        q=f"name='{name}' and '{parent_id}' in parents and trashed=false",
        fields="files(id)",
        spaces="drive",
    ).execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def upload_newsletter(pdf_path: str, year: str, month: str) -> str:
    """PDF를 ROOT/{year}/{month}/ 에 업로드하고 file id 반환. 같은 이름 파일이 있으면 덮어씀."""
    service = _build_service()

    year_folder_id = _get_or_create_folder(service, year, ROOT_FOLDER_ID)
    month_folder_id = _get_or_create_folder(service, month, year_folder_id)

    file_name = Path(pdf_path).name
    media = MediaFileUpload(pdf_path, mimetype="application/pdf", resumable=True)

    existing_id = _find_file(service, file_name, month_folder_id)
    if existing_id:
        result = service.files().update(
            fileId=existing_id,
            media_body=media,
            fields="id",
        ).execute()
        file_id = result["id"]
        logger.info(f"[GDriveUploader] 덮어쓰기 완료: {file_name} → {year}/{month}/ (id={file_id})")
    else:
        result = service.files().create(
            body={"name": file_name, "parents": [month_folder_id]},
            media_body=media,
            fields="id",
        ).execute()
        file_id = result["id"]
        logger.info(f"[GDriveUploader] 업로드 완료: {file_name} → {year}/{month}/ (id={file_id})")

    return file_id
