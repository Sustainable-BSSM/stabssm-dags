from abc import ABC, abstractmethod

import polars as pl


class BumaWikiNeo4jNodeRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame, db_name: str) -> None:
        raise NotImplementedError
