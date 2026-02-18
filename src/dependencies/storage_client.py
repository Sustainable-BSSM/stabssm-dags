from src.core.client.storage import StorageClient, FakeStorageClient


def get_storage_client() -> StorageClient:
    return FakeStorageClient()