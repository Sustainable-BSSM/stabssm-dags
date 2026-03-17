import logging
import re
from asyncio import Semaphore, gather, to_thread

from src.common.config.newslatter import NaverConfig
from src.core.crawler import Crawler
from src.core.newslatter.model import NewsInfo
from src.core.requester import Requester

logger = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(text: str) -> str:
    return _TAG_RE.sub("", text).strip()


class NaverNewsCrawler(Crawler):
    API_URL = "https://openapi.naver.com/v1/search/news.json"
    MAX_DISPLAY = 100
    MAX_START = 1000

    def __init__(self, requester: Requester, query: str, sort: str = "date"):
        super().__init__(requester)
        self.query = query
        self.sort = sort
        self._headers = {
            "X-Naver-Client-Id": NaverConfig.CLIENT_ID,
            "X-Naver-Client-Secret": NaverConfig.CLIENT_SECRET,
        }

    async def run(self, semaphore: Semaphore) -> list[NewsInfo]:
        first_data = await to_thread(self._fetch, 1)
        first_items = self._parse(first_data)
        if not first_items:
            return []

        total = first_data.get("total", 0)
        remaining_starts = range(
            1 + self.MAX_DISPLAY,
            min(total, self.MAX_START) + 1,
            self.MAX_DISPLAY,
        )

        async def fetch_page(start: int) -> list[NewsInfo]:
            async with semaphore:
                data = await to_thread(self._fetch, start)
                return self._parse(data)

        pages = await gather(*[fetch_page(s) for s in remaining_starts])
        all_items = first_items + [item for page in pages for item in page]
        logger.info(f"[{self.query}] 총 {len(all_items)}건 수집")
        return all_items

    def _fetch(self, start: int = 1) -> dict:
        params = {
            "query": self.query,
            "display": self.MAX_DISPLAY,
            "start": start,
            "sort": self.sort,
        }
        resp = self.requester.get(url=self.API_URL, headers=self._headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def _parse(self, fetched_data: dict) -> list[NewsInfo]:
        return [
            NewsInfo(
                title=_strip_tags(item.get("title", "")),
                original_link=item.get("originallink", ""),
                link=item.get("link", ""),
                description=_strip_tags(item.get("description", "")),
                pub_date=item.get("pubDate", ""),
                query=self.query,
            )
            for item in fetched_data.get("items", [])
        ]
