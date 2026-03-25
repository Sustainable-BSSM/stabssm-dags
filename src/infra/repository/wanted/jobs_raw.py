import logging

import polars as pl

from src.common.config.s3 import S3Config
from src.core.repository.wanted.jobs_raw import WantedJobsRawRepository
from src.infra.duckdb import create_conn

logger = logging.getLogger(__name__)


class DuckDBWantedJobsRawRepository(WantedJobsRawRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    # bronze 스키마가 변경되더라도 누락 컬럼을 null로 채워 안전하게 읽음
    _EXPECTED_COLUMNS: dict[str, pl.DataType] = {
        "id":                    pl.Int64,
        "company_id":            pl.Int64,
        "company_name":          pl.String,
        "position":              pl.String,
        "location":              pl.String,
        "district":              pl.String,
        "employment_type":       pl.String,
        "annual_from":           pl.Int64,
        "annual_to":             pl.Int64,
        "is_newbie":             pl.Boolean,
        "category_id":           pl.Int64,
        "additional_apply_type": pl.String,
        "requirements":          pl.String,
    }

    def read(self, ds: str) -> pl.DataFrame:
        try:
            df = self._conn.execute(f"""
                SELECT *
                FROM read_json(
                    's3://{self._bucket}/wanted/bronze/jobs/dt={ds}/jobs.json',
                    format      = 'newline_delimited',
                    auto_detect = true
                )
            """).pl()
        except Exception as e:
            logger.warning(f"[DuckDBWantedJobsRawRepository] bronze 파일 없음 (ds={ds}): {e}")
            return pl.DataFrame()

        # 누락된 컬럼을 null로 보정
        for col, dtype in self._EXPECTED_COLUMNS.items():
            if col not in df.columns:
                logger.warning(f"[DuckDBWantedJobsRawRepository] 누락 컬럼 보정: {col}")
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

        return df.select(list(self._EXPECTED_COLUMNS.keys()))
