from abc import ABC, abstractmethod

import polars as pl


class NewsSilverRepository(ABC):

    @abstractmethod
    def read(self, week: str) -> pl.DataFrame:
        raise NotImplementedError
