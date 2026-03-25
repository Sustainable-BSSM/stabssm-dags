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

    def read(self, ds: str) -> pl.DataFrame:
        try:
            return self._conn.execute(f"""
                SELECT
                    id,
                    company_id,
                    company_name,
                    position,
                    location,
                    district,
                    employment_type,
                    annual_from,
                    annual_to,
                    is_newbie,
                    category_id,
                    additional_apply_type,
                    requirements
                FROM read_json(
                    's3://{self._bucket}/wanted/bronze/jobs/dt={ds}/jobs.json',
                    format      = 'newline_delimited',
                    auto_detect = true
                )
            """).pl()
        except Exception as e:
            logger.warning(f"[DuckDBWantedJobsRawRepository] bronze 파일 없음 (ds={ds}): {e}")
            return pl.DataFrame()
