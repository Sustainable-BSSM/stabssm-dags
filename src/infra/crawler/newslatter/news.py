import logging
import re
from asyncio import Semaphore, sleep, to_thread

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
        all_items: list[NewsInfo] = []

        for start in range(1, self.MAX_START + 1, self.MAX_DISPLAY):
            async with semaphore:
                data = await self._fetch_with_backoff(start)

            if data is None:
                break

            items = self._parse(data)
            if not items:
                break

            all_items.extend(items)

            if len(items) < self.MAX_DISPLAY:
                break

            await sleep(0.5)

        logger.info(f"[{self.query}] 총 {len(all_items)}건 수집")
        return all_items

    async def _fetch_with_backoff(self, start: int) -> dict | None:
        for attempt in range(4):
            try:
                return await to_thread(self._fetch, start)
            except Exception as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"[{self.query}] 429 rate limit, {wait}s 대기 (attempt={attempt + 1})")
                    await sleep(wait)
                else:
                    logger.warning(f"[{self.query}] start={start} 실패: {e}")
                    return None
        return None

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
