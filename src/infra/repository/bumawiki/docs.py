import polars as pl

from src.common.config.s3 import S3Config
from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.infra.duckdb import create_conn


class DuckDBBumaWikiDocsRepository(BumaWikiDocsRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    async def get(self, ds: str) -> pl.DataFrame:
        try:
            return self._conn.execute(f"""
                SELECT * FROM read_parquet('s3://{self._bucket}/silver/bumawiki/docs/dt={ds}/*.parquet')
            """).pl()
        except Exception:
            return pl.DataFrame()

    def save(self, df: pl.DataFrame, ds: str) -> None:
        self._conn.register("silver_df", df)
        self._conn.execute(f"""
            COPY silver_df
            TO 's3://{self._bucket}/silver/bumawiki/docs/dt={ds}/part-0000.parquet'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
