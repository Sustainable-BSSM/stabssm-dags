"""
로컬에서 1회만 실행 — Google OAuth 토큰 생성

사전 준비:
  1. Google Cloud Console → API 및 서비스 → 사용자 인증 정보
  2. OAuth 2.0 클라이언트 ID 생성 (데스크톱 앱)
  3. credentials.json 다운로드 후 이 스크립트와 같은 디렉토리에 위치

실행:
  python scripts/generate_gdrive_token.py

출력된 JSON을 .env의 GOOGLE_OAUTH_TOKEN_JSON 값으로 붙여넣기
"""

import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]

flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
creds = flow.run_local_server(port=0)

token_json = creds.to_json()
print("\n=== .env에 아래 값을 붙여넣기 ===")
print(f"GOOGLE_OAUTH_TOKEN_JSON={token_json}")
