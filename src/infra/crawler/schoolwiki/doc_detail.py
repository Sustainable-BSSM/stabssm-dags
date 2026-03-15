import json

from src.core.crawler import Crawler
from src.core.requester import Requester


class SchoolwikiDocDetailCrawler(Crawler):
    SITE_BASE = "https://schoolwiki.org"
    SUBDOMAIN = "bssm-hs-kr"
    RSC_HEADERS = {"RSC": "1", "Accept": "text/x-component"}

    def __init__(self, requester: Requester):
        super().__init__(requester)

    def _fetch(self, slug: str):
        url = f"{self.SITE_BASE}/s/{self.SUBDOMAIN}/w/{slug}"
        resp = self.requester.get(url=url, headers=self.RSC_HEADERS)
        resp.raise_for_status()
        return resp

    def _parse(self, fetched_data) -> dict | None:
        text = fetched_data.content.decode('utf-8')
        # RSC 스트림에서 {"content":{"type":"doc",...},"title":"...",...} 패턴 추출
        key = '{"content":{"type":"doc"'
        idx = text.find(key)
        if idx == -1:
            return None
        try:
            document, _ = json.JSONDecoder().raw_decode(text, idx)
            return document if isinstance(document, dict) else None
        except json.JSONDecodeError:
            return None
