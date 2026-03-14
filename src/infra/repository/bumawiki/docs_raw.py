import polars as pl

from src.common.config.s3 import S3Config
from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.infra.duckdb import create_conn


class DuckDBBumaWikiDocsRawRepository(BumaWikiDocsRawRepository):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    def read(self, ds: str) -> pl.DataFrame:
        return self._conn.execute(f"""
            SELECT
                id,
                title,
                contents,
                docsType       AS docstype,
                lastModifiedAt AS lastmodifiedat,
                enroll,
                TO_JSON(contributors) AS contributors,
                status,
                version,
                thumbnail,
                docsDetail     AS docsdetail
            FROM read_json(
                's3://{self._bucket}/bronze/bumawiki/docs/dt={ds}/*',
                format       = 'newline_delimited',
                auto_detect  = true
            )
        """).pl()

    def exists(self, ds: str, title: str) -> bool:
        try:
            result = self._conn.execute(f"""
                SELECT COUNT(*) FROM read_json_auto('s3://{self._bucket}/bronze/bumawiki/docs/dt={ds}/*.json')
                WHERE title = '{title}'
            """).fetchone()
            return result[0] > 0
        except Exception:
            return False

