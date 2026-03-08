import polars as pl

from src.common.config.s3 import S3Config
from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.infra.duckdb import create_conn


class DuckDBBumaWikiDocsRepository(BumaWikiDocsRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    async def get(self, ds: str) -> pl.DataFrame:
        return self._conn.execute(f"""
            SELECT * FROM read_parquet('s3://{self._bucket}/silver/bumawiki/docs/dt={ds}/*.parquet')
        """).pl()
