from abc import ABC, abstractmethod


class BumaWikiDocsRawRepository(ABC):

    @abstractmethod
    def save(self, ds: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def exists(self, ds: str, title: str) -> bool:
        raise NotImplementedError
