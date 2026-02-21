import polars as pl

from src.common.config.s3 import S3Config
from src.common.enum.bumawiki.docs_type import BumaWikiDocsType
from src.common.util.bssm_generation_calculator import BSSMGenerationCalculator
from src.core.jobs import Job


class TransformDocsToParquetJob(Job):
    def __init__(self, bucket_name: str = S3Config.BUCKET_NAME):
        self.bucket = bucket_name

    def __call__(self, ds: str):
        year = int(ds[:4])
        now_generation = BSSMGenerationCalculator.calculate(year=year)

        for docs_type in BumaWikiDocsType:
            doc_type = docs_type.value

            for generation in range(1, now_generation + 1):
                upstream = f"s3://{self.bucket}/bronze/bumawiki/{doc_type}/dt={ds}/{generation}/{doc_type}.json"
                downstream = f"s3://{self.bucket}/silver/bumawiki/{doc_type}/dt={ds}/{generation}/part-0000.parquet"

                try:
                    df = pl.read_json(upstream)
                except Exception as exc:
                    continue

                if df.height == 0:
                    continue

                df.write_parquet(downstream, compression="snappy")