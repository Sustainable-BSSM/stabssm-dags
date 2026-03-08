import argparse

from src.common.config.s3 import S3Config
from src.core.jobs import Job
from src.infra.duckdb import create_conn


class TransformDocsDetailToParquetJob(Job):

    def __init__(self, bucket: str = S3Config.BUCKET_NAME):
        self._bucket = bucket
        self._conn = create_conn()

    def __call__(self, ds: str):
        df = self._conn.execute(f"""
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

        if df.is_empty():
            return

        self._conn.register("silver_df", df)
        self._conn.execute(f"""
            COPY silver_df
            TO 's3://{self._bucket}/silver/bumawiki/docs/dt={ds}/part-0000.parquet'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)


def run_job(ds: str):
    job = TransformDocsDetailToParquetJob()
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
