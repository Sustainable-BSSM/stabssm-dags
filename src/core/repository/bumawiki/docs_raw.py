from abc import ABC, abstractmethod

import polars as pl


class BumaWikiDocsRawRepository(ABC):

    @abstractmethod
    def read(self, ds: str) -> pl.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def exists(self, ds: str, title: str) -> bool:
        raise NotImplementedError
