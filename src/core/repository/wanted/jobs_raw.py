from abc import ABC, abstractmethod

import polars as pl


class WantedJobsRawRepository(ABC):

    @abstractmethod
    def read(self, ds: str) -> pl.DataFrame:
        raise NotImplementedError
