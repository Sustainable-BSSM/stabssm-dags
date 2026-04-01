from abc import ABC, abstractmethod

import polars as pl


class NewsRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame, week: str) -> None:
        raise NotImplementedError
