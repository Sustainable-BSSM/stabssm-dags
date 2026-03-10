from abc import ABC, abstractmethod

import polars as pl


class BumaWikiNeo4jEdgeRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame, db_name: str) -> None:
        raise NotImplementedError
