from abc import ABC, abstractmethod


class NewsScorer(ABC):

    @abstractmethod
    async def score(self, article: dict) -> dict:
        """
        Returns:
            dict with keys: relevance_score (float), publisher_score (float), category (str)
        """
        raise NotImplementedError
