import argparse
import calendar
import json
import logging
from asyncio import Semaphore, gather, run
from datetime import date
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


def _week_to_date_range(week: str) -> tuple[date, date]:
    """'2026-03-02' → (date(2026,3,8), date(2026,3,14))"""
    year, month, week_num = int(week.split('-')[0]), int(week.split('-')[1]), int(week.split('-')[2])
    start_day = (week_num - 1) * 7 + 1
    last_day = calendar.monthrange(year, month)[1]
    end_day = min(week_num * 7, last_day)
    return date(year, month, start_day), date(year, month, end_day)


def _in_range(pub_date: str, start: date, end: date) -> bool:
    try:
        return start <= parsedate_to_datetime(pub_date).date() <= end
    except Exception:
        return False


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

    def __call__(self, week: str):
        logger.info(f"네이버 뉴스 수집 시작 (week={week}, queries={self.queries})")
        run(self._run(week))
        logger.info("네이버 뉴스 수집 완료")

    async def _run(self, week: str):
        start, end = _week_to_date_range(week)
        logger.info(f"수집 범위: {start} ~ {end}")

        semaphore = Semaphore(3)
        results = await gather(*[self._fetch_query(query, semaphore) for query in self.queries])

        seen: set[str] = set()
        items: list[dict] = []
        for articles in results:
            for article in articles:
                if article.link in seen:
                    continue
                if not _in_range(article.pub_date, start, end):
                    continue
                seen.add(article.link)
                items.append(article.to_dict())

        logger.info(f"범위 내 중복 제거 후 총 {len(items)}건")

        if items:
            year, month, week_num = week.split('-')
            key = f"newslatter/bronze/news/year={year}/month={month}/week={week_num}/news.json"
            items = self._merge_with_existing(key, items)
            self.storage_client.upload(key=key, value=items)
            logger.info(f"[DONE] {week} - {len(items)}건 업로드")

    def _merge_with_existing(self, key: str, new_items: list[dict]) -> list[dict]:
        existing = self.storage_client.get(key)
        if not existing:
            return new_items
        existing_links = {item["link"] for item in existing}
        deduped_new = [item for item in new_items if item["link"] not in existing_links]
        merged = existing + deduped_new
        logger.info(f"기존 {len(existing)}건 + 신규 {len(deduped_new)}건 = {len(merged)}건")
        return merged

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


def run_job(week: str):
    storage_client = get_storage_client()
    job = CollectNaverNewsJob(storage_client=storage_client)
    job(week=week)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
