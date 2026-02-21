from src.core.client.storage import StorageClient, FakeStorageClient
from src.infra.client.storage.s3 import S3StorageClient


def get_storage_client() -> StorageClient:
    # return FakeStorageClient()
    return S3StorageClient()