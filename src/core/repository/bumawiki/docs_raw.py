from abc import ABC, abstractmethod


class BumaWikiDocsRawRepository(ABC):

    @abstractmethod
    def exists(self, ds: str, title: str) -> bool:
        raise NotImplementedError
