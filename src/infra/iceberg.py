from pyiceberg.catalog.glue import GlueCatalog

from src.common.config.s3 import S3Config


def create_catalog() -> GlueCatalog:
    return GlueCatalog("glue", **{
        "s3.access-key-id": S3Config.ACCESS_KEY,
        "s3.secret-access-key": S3Config.SECRET_KEY,
        "s3.region": S3Config.REGION,
        "region_name": S3Config.REGION,
    })
