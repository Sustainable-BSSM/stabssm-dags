import json

from src.core.crawler import Crawler
from src.core.requester import Requester
from src.core.schoolwiki.model import SchoolwikiCategory


class SchoolwikiDocsCrawler(Crawler):
    SITE_BASE = "https://schoolwiki.org"
    SUBDOMAIN = "bssm-hs-kr"
    RSC_HEADERS = {"RSC": "1", "Accept": "text/x-component"}

    def __init__(self, requester: Requester, category: SchoolwikiCategory | str):
        super().__init__(requester)
        if isinstance(category, str):
            category = SchoolwikiCategory(category.upper())
        self.category = category
        self.url = f"{self.SITE_BASE}/s/{self.SUBDOMAIN}/{category.url_slug}"

    def _fetch(self):
        resp = self.requester.get(url=self.url, headers=self.RSC_HEADERS)
        resp.raise_for_status()
        return resp

    def _parse(self, fetched_data) -> dict[str, list[dict]]:
        text = fetched_data.content.decode('utf-8')
        docs = self._extract_documents(text)
        return self._group_by_year(docs)

    def _extract_documents(self, text: str) -> list[dict]:
        key = '"documents":['
        idx = text.find(key)
        if idx == -1:
            return []
        start = idx + len(key) - 1
        try:
            documents, _ = json.JSONDecoder().raw_decode(text, start)
        except json.JSONDecodeError:
            return []
        return documents if isinstance(documents, list) else []

    def _group_by_year(self, docs: list[dict]) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        for doc in docs:
            year = str(doc.get("year", "unknown"))
            result.setdefault(year, []).append(doc)
        return result
