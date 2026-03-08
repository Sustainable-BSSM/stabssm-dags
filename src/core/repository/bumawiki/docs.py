from abc import ABC, abstractmethod

import polars as pl


class BumaWikiDocsRepository(ABC):

    @abstractmethod
    async def get(self, ds: str) -> pl.DataFrame:
        raise NotImplementedError
