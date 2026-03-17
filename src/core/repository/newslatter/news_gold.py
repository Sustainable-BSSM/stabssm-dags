from abc import ABC, abstractmethod

import polars as pl


class NewsGoldRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame) -> None:
        raise NotImplementedError
