from typing import Any

import boto3
from botocore.exceptions import ClientError

from src.common.config.s3 import S3Config
from src.common.util.codec import JsonSerializer, JsonDeserializer
from src.core.client.storage import StorageClient


class S3StorageClient(StorageClient):
    def __init__(
            self,
            region_name: str = S3Config.REGION,
            aws_access_key_id: str = S3Config.ACCESS_KEY,
            aws_secret_access_key: str = S3Config.SECRET_KEY,
            bucket_name: str = S3Config.BUCKET_NAME,
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

    def list_keys(self, prefix: str) -> list[str]:
        keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def upload_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream"):
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data,
            ContentType=content_type,
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

    def get_bytes(self, key: str) -> bytes | None:
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return obj["Body"].read()
        except ClientError:
            return None
