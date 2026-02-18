from src.core.crawler import Crawler
from src.core.requester import Requester

class BumawikiStudentCrawler(Crawler):

    def __init__(
            self,
            requester : Requester,
    ):
        super().__init__(requester)
        self.base_url = "https://buma.wiki/api/docs/student"


    def _fetch(self):
        fetch_data = self.requester.get(url=self.base_url)
        fetch_data.raise_for_status()
        return fetch_data

    def _parse(self, fetched_data):
        fetched_json = fetched_data.json()
        return fetched_json['data']
