from abc import ABC, abstractmethod

from src.core.requester import Requester

class Crawler(ABC):
    def __init__(
            self,
            requester : Requester
    ):
        self.requester = requester

    def run(self, *args):
        fetched_data = self._fetch(*args)
        parsed_data = self._parse(fetched_data)
        return parsed_data

    @abstractmethod
    def _fetch(self, *args):
        raise NotImplementedError

    @abstractmethod
    def _parse(self, fetched_data):
        raise NotImplementedError