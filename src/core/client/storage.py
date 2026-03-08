from abc import ABC, abstractmethod
from typing import Optional, Any


class StorageClient(ABC):

    @abstractmethod
    def upload(
            self,
            key : str,
            value : Optional[Any]
    ):
        raise NotImplementedError

    @abstractmethod
    def get(self, key : str):
        raise NotImplementedError

    @abstractmethod
    def get_bytes(self, key: str) -> Optional[bytes]:
        raise NotImplementedError

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def upload_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream"):
        raise NotImplementedError

class FakeStorageClient(StorageClient):
    def upload(
            self,
            key : str,
            value : Optional[Any]
    ):
        from pprint import pprint
        pprint(f"Uploading {key} to {value}")

    def get(self, key: str):
        return {}

    def get_bytes(self, key: str) -> Optional[bytes]:
        return None

    def list_keys(self, prefix: str) -> list[str]:
        return []

    def upload_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream"):
        pass

