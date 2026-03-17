import argparse
import logging
from asyncio import Semaphore, gather, run
from collections import defaultdict
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import List

from src.core.client.storage import StorageClient
from src.core.jobs import Job
from src.core.requester import Requester
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.newslatter.news import NaverNewsCrawler
from src.infra.requester.http import HttpRequester

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

QUERIES = [
    "부산소프트웨어마이스터고",
    "부산 SW 마이스터고",
    "부산 소프트웨어 마이스터고",
]

def _pub_date_to_partition(pub_date: str) -> tuple[str, str, str]:
    """RFC 2822 pubDate → (year, month, week_of_month) 파티션 튜플"""
    try:
        dt: datetime = parsedate_to_datetime(pub_date)
        week_of_month = (dt.day - 1) // 7 + 1
        return str(dt.year), f"{dt.month:02d}", f"{week_of_month:02d}"
    except Exception:
        return "unknown", "unknown", "unknown"


class CollectNaverNewsJob(Job):
    def __init__(
            self,
            storage_client: StorageClient,
            queries: List[str] = QUERIES,
            requester: Requester = HttpRequester(),
    ):
        self.storage_client = storage_client
        self.queries = queries
        self.requester = requester

    def __call__(self):
        logger.info(f"네이버 뉴스 수집 시작 (queries={self.queries})")
        run(self._run())
        logger.info("네이버 뉴스 수집 완료")

    async def _run(self):
        semaphore = Semaphore(3)
        results = await gather(*[self._fetch_query(query, semaphore) for query in self.queries])

        seen: set[str] = set()
        by_partition: dict[tuple, list[dict]] = defaultdict(list)
        for articles in results:
            for article in articles:
                if article.link in seen:
                    continue
                seen.add(article.link)
                partition = _pub_date_to_partition(article.pub_date)
                by_partition[partition].append(article.to_dict())

        logger.info(f"중복 제거 후 총 {len(seen)}건")

        for (year, month, week), items in by_partition.items():
            key = f"newslatter/bronze/news/year={year}/month={month}/week={week}/news.json"
            self.storage_client.upload(key=key, value=items)
            logger.info(f"[DONE] {year}/{month}/{week} - {len(items)}건 업로드")

    async def _fetch_query(self, query: str, semaphore: Semaphore):
        crawler = NaverNewsCrawler(requester=self.requester, query=query)
        try:
            articles = await crawler.run(semaphore)
            if not articles:
                logger.info(f"[SKIP] '{query}' - 결과 없음")
            return articles
        except Exception as e:
            logger.warning(f"[FAIL] '{query}': {e}")
            return []


def run_job():
    storage_client = get_storage_client()
    job = CollectNaverNewsJob(storage_client=storage_client)
    job()


if __name__ == "__main__":
    run_job()
