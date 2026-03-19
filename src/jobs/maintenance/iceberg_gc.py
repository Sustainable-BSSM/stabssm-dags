import argparse
import logging
from datetime import datetime, timedelta, timezone

from src.common.config.glue import GlueConfig
from src.infra.iceberg import create_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = [
    (GlueConfig.SILVER_DATABASE, "newslatter_school"),
    (GlueConfig.SILVER_DATABASE, "newslatter_it"),
    (GlueConfig.GOLD_DATABASE, "newslatter_school"),
    (GlueConfig.GOLD_DATABASE, "newslatter_it"),
]


def run_gc(older_than_days: int = 7):
    catalog = create_catalog()
    expire_before = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    for table_id in TABLES:
        try:
            table = catalog.load_table(table_id)
        except Exception:
            logger.info(f"[IcebergGC] 테이블 없음, 스킵: {table_id}")
            continue

        logger.info(f"[IcebergGC] {table_id} — expire snapshots older than {expire_before.date()}")
        table.maintenance.expire_snapshots().older_than(expire_before).commit()
        logger.info(f"[IcebergGC] {table_id} 완료")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--older-than-days", type=int, default=7)
    args = p.parse_args()
    run_gc(older_than_days=args.older_than_days)
