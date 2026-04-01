from abc import ABC, abstractmethod

import polars as pl


class WantedJobsRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame, ds: str) -> None:
        raise NotImplementedError
