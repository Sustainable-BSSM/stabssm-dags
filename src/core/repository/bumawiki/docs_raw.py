from abc import ABC, abstractmethod


class BumaWikiDocsRawRepository(ABC):

    @abstractmethod
    def save(self, ds: str) -> None:
        raise NotImplementedError
