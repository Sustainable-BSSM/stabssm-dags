#!/bin/bash
set -e

echo "[init_variables] Airflow Variables 설정 시작"

vars=(
  "S3_ACCESS_KEY:${S3_ACCESS_KEY}"
  "S3_SECRET_KEY:${S3_SECRET_KEY}"
  "S3_BUCKET_NAME:${S3_BUCKET_NAME}"
  "S3_REGION:${S3_REGION}"
  "OPENROUTER_API_KEY:${OPENROUTER_API_KEY}"
  "NAVER_NEWS_CLIENT_ID:${NAVER_NEWS_CLIENT_ID}"
  "NAVER_NEWS_SECRET:${NAVER_NEWS_SECRET}"
  "NEO4J_URI:${NEO4J_URI}"
  "NEO4J_USER:${NEO4J_USER}"
  "NEO4J_PASSWORD:${NEO4J_PASSWORD}"
  "LANGCHAIN_API_KEY:${LANGSMITH_API_KEY}"
)

for entry in "${vars[@]}"; do
  key="${entry%%:*}"
  value="${entry#*:}"
  airflow variables set "$key" "$value"
  echo "[init_variables] SET $key"
done

# JSON 값은 별도 처리 (콜론 포함)
if [ -n "${GOOGLE_SERVICE_ACCOUNT_JSON}" ]; then
  airflow variables set "GOOGLE_SERVICE_ACCOUNT_JSON" "${GOOGLE_SERVICE_ACCOUNT_JSON}"
  echo "[init_variables] SET GOOGLE_SERVICE_ACCOUNT_JSON"
fi

echo "[init_variables] 완료"
