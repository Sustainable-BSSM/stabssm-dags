import json
from typing import Optional, Any

import botocore
from botocore.exceptions import ClientError

from src.common.config.s3 import S3Config
from src.common.util.codec import JsonSerializer, JsonDeserializer
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
            value: Any
    ):
        body = "\n".join(
            JsonSerializer.serialize(row) for row in value
        ).encode("utf-8")
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )

    def get(self, key: str):
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            raw_str = obj["Body"].read().decode("utf-8")
            data = [JsonDeserializer.deserialize(line) for line in raw_str.splitlines() if line.strip()]
            return data

        except ClientError as exc:
            return None