from src.common.config.s3 import S3Config
from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.infra.duckdb import create_conn


class DuckDBBumaWikiDocsRawRepository(BumaWikiDocsRawRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    def save(self, ds: str) -> None:
        self._conn.execute(f"""
            COPY (
                SELECT *
                FROM read_json_auto('s3://{self._bucket}/bronze/bumawiki/docs/dt={ds}/*.json')
            )
            TO 's3://{self._bucket}/bronze/bumawiki/docs/dt={ds}/part-0000.parquet'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
