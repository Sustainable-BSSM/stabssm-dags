from abc import ABC, abstractmethod

import polars as pl


class NewsRawRepository(ABC):

    @abstractmethod
    def read(self, week: str) -> pl.DataFrame:
        raise NotImplementedError
