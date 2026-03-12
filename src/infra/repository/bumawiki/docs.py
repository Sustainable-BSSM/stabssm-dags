import logging
import polars as pl

from src.common.config.s3 import S3Config
from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.infra.duckdb import create_conn

logger = logging.getLogger(__name__)


class DuckDBBumaWikiDocsRepository(BumaWikiDocsRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()
        logger.info(f"[DocsRepository] bucket={self._bucket!r}")

    async def get(self, ds: str) -> pl.DataFrame:
        path = f"s3://{self._bucket}/silver/bumawiki/docs/dt={ds}/*.parquet"
        logger.info(f"[DocsRepository] reading parquet: {path}")
        try:
            df = self._conn.execute(f"""
                SELECT * FROM read_parquet('{path}')
            """).pl()
            logger.info(f"[DocsRepository] loaded {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"[DocsRepository] failed to read parquet: {e}")
            return pl.DataFrame()

    def save(self, df: pl.DataFrame, ds: str) -> None:
        self._conn.register("silver_df", df)
        self._conn.execute(f"""
            COPY silver_df
            TO 's3://{self._bucket}/silver/bumawiki/docs/dt={ds}/part-0000.parquet'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
