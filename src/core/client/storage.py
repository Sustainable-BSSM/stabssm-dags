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

