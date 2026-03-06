import argparse
import io
import json

import polars as pl

from src.common.config.s3 import S3Config
from src.core.client.storage import StorageClient
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client


class TransformDocsDetailToParquetJob(Job):

    def __init__(self, storage_client: StorageClient, bucket_name: str = S3Config.BUCKET_NAME):
        self.storage_client = storage_client
        self.bucket = bucket_name

    def __call__(self, ds: str):
        prefix = f"bronze/bumawiki/docs/dt={ds}/"
        keys = self.storage_client.list_keys(prefix=prefix)

        if not keys:
            return

        rows = []
        for key in keys:
            data = self.storage_client.get(key)
            if not data:
                continue
            doc = data[0]
            doc["contributors"] = json.dumps(doc.get("contributors", []), ensure_ascii=False)
            rows.append(doc)

        if not rows:
            return

        df = pl.DataFrame(rows, infer_schema_length=len(rows))

        buf = io.BytesIO()
        df.write_parquet(buf, compression="snappy")

        key = f"silver/bumawiki/docs/dt={ds}/part-0000.parquet"
        self.storage_client.upload_bytes(key=key, data=buf.getvalue())


def run_job(ds: str):
    storage_client = get_storage_client()
    job = TransformDocsDetailToParquetJob(storage_client=storage_client)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
