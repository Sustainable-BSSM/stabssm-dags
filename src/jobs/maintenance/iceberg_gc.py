import argparse
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3

from src.common.config.glue import GlueConfig
from src.common.config.s3 import S3Config
from src.infra.iceberg import create_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = [
    (GlueConfig.SILVER_DATABASE, "newslatter_school"),
    (GlueConfig.SILVER_DATABASE, "newslatter_it"),
    (GlueConfig.GOLD_DATABASE, "newslatter_school"),
    (GlueConfig.GOLD_DATABASE, "newslatter_it"),
]


def _referenced_files(table) -> set[str]:
    """모든 현재 스냅샷에서 참조 중인 파일 경로 수집."""
    refs: set[str] = set()
    for snapshot in table.metadata.snapshots:
        for manifest in snapshot.manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io):
                refs.add(entry.data_file.file_path)
    return refs


def _delete_orphan_files(table, older_than: datetime) -> int:
    """S3에서 Iceberg 메타데이터에 없는 고아 parquet 파일 삭제."""
    location = table.metadata.location  # e.g. s3://bucket/iceberg/ns/table
    parsed = urlparse(location)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/") + "/data/"

    referenced = _referenced_files(table)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3Config.ACCESS_KEY,
        aws_secret_access_key=S3Config.SECRET_KEY,
        region_name=S3Config.REGION,
    )

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            file_path = f"s3://{bucket}/{key}"
            last_modified = obj["LastModified"]
            if file_path not in referenced and last_modified < older_than:
                s3.delete_object(Bucket=bucket, Key=key)
                logger.info(f"[IcebergGC] 고아 파일 삭제: {key}")
                deleted += 1

    return deleted


def run_gc(older_than_days: int = 7):
    catalog = create_catalog()
    expire_before = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    # 고아 파일은 1시간 이상 된 것만 삭제 (진행 중인 write와 race condition 방지)
    orphan_before = datetime.now(timezone.utc) - timedelta(hours=1)

    for table_id in TABLES:
        try:
            table = catalog.load_table(table_id)
        except Exception:
            logger.info(f"[IcebergGC] 테이블 없음, 스킵: {table_id}")
            continue

        logger.info(f"[IcebergGC] {table_id} — expire snapshots older than {expire_before.date()}")
        table.maintenance.expire_snapshots().older_than(expire_before).commit()

        deleted = _delete_orphan_files(table, orphan_before)
        logger.info(f"[IcebergGC] {table_id} 완료 (고아 파일 {deleted}개 삭제)")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--older-than-days", type=int, default=7)
    args = p.parse_args()
    run_gc(older_than_days=args.older_than_days)
