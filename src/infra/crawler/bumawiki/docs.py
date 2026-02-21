

from src.core.crawler import Crawler
from src.core.requester import Requester


class BumawikiDocsCrawler(Crawler):

    def __init__(
            self,
            requester : Requester,
    ):
        super().__init__(requester)
        self.base_url = "https://buma.wiki/api/docs/find/title/{target}"


    def _fetch(self, target : str):
        fetch_data = self.requester.get(
            url=self.base_url.format(target=target)
        )
        fetch_data.raise_for_status()
        return fetch_data

    def _parse(self, fetched_data):
        fetched_json = fetched_data.json()
        return fetched_json
