import logging

import polars as pl

from src.common.config.s3 import S3Config
from src.core.repository.newslatter.news_raw import NewsRawRepository
from src.infra.duckdb import create_conn

logger = logging.getLogger(__name__)


class DuckDBNewsRawRepository(NewsRawRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME, source: str = "school"):
        self._bucket = bucket
        self._source = source
        self._conn = create_conn()

    def read(self, week: str) -> pl.DataFrame:
        year, month, week_num = week.split('-')
        try:
            return self._conn.execute(f"""
                SELECT
                    title,
                    original_link,
                    link,
                    description,
                    pub_date,
                    query
                FROM read_json(
                    's3://{self._bucket}/newslatter/bronze/{self._source}/year={year}/month={month}/week={week_num}/news.json',
                    format      = 'newline_delimited',
                    auto_detect = true
                )
            """).pl()
        except Exception as e:
            logger.warning(f"[DuckDBNewsRawRepository] bronze 파일 없음 (week={week}): {e}")
            return pl.DataFrame()
