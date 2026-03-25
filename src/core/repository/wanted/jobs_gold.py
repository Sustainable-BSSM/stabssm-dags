from abc import ABC, abstractmethod

import polars as pl


class WantedJobsGoldRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame, ds: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def read_top(self, ds: str, n: int = 5) -> pl.DataFrame:
        raise NotImplementedError
