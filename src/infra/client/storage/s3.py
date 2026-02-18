from typing import Optional, Any

from src.common.config.s3 import S3Config
from src.core.client.storage import StorageClient
import boto3

class S3StorageClient(StorageClient):
    def __init__(
            self,
            region_name : str = S3Config.REGION,
            aws_access_key_id : str = S3Config.ACCESS_KEY,
            aws_secret_access_key : str = S3Config.SECRET_KEY,
            bucket_name : str = S3Config.BUCKET_NAME,
    ):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )


    def upload(
            self,
            key: str,
            value: Optional[Any]
    ):
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=value,
            ContentType="application/json",
        )