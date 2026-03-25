from abc import ABC, abstractmethod

import polars as pl


class WantedJobsGoldRepository(ABC):

    @abstractmethod
    def save(self, df: pl.DataFrame, ds: str) -> None:
        raise NotImplementedError
