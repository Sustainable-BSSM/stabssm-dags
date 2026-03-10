from abc import ABC, abstractmethod

import polars as pl


class BumaWikiDocsRepository(ABC):

    @abstractmethod
    async def get(self, ds: str) -> pl.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def save(self, df: pl.DataFrame, ds: str) -> None:
        raise NotImplementedError
