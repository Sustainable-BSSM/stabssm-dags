import warnings

from src.core.crawler import Crawler
from src.core.requester import Requester


class BumawikiAccidentCrawler(Crawler):
    """
    .. deprecated::
        buma.wiki 서비스가 schoolwiki.org로 이전됨.
        SchoolwikiDocsCrawler(category="INCIDENT") 사용 권장.
    """

    def __init__(
            self,
            requester: Requester,
    ):
        warnings.warn(
            "BumawikiAccidentCrawler는 deprecated입니다. SchoolwikiDocsCrawler를 사용하세요.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(requester)
        self.base_url = "https://buma.wiki/api/docs/accident"

    def _fetch(self):
        fetch_data = self.requester.get(url=self.base_url)
        fetch_data.raise_for_status()
        return fetch_data

    def _parse(self, fetched_data):
        fetched_json = fetched_data.json()
        return fetched_json['data']
