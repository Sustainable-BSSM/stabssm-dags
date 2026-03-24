import argparse
import calendar
import logging
from asyncio import Semaphore, run
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



class CollectNaverNewsJob(Job):
    def __init__(
            self,
            storage_client: StorageClient,
            queries: List[str] = QUERIES,
            requester: Requester = HttpRequester(),
            source: str = "school",
    ):
        self.storage_client = storage_client
        self.queries = queries
        self.requester = requester
        self.source = source

    def __call__(self, week: str):
        logger.info(f"네이버 뉴스 수집 시작 (week={week}, queries={self.queries})")
        run(self._run(week))
        logger.info("네이버 뉴스 수집 완료")

    async def _run(self, week: str):
        start, end = _week_to_date_range(week)
        logger.info(f"수집 범위: {start} ~ {end}")

        semaphore = Semaphore(1)
        seen: set[str] = set()
        items: list[dict] = []
        for query in self.queries:
            articles = await self._fetch_query(query, semaphore)
            for article in articles:
                article_key = article.original_link or article.link
                if article_key in seen:
                    continue
                if not self._in_date_range(article.pub_date, start, end):
                    continue
                seen.add(article_key)
                items.append(article.to_dict())

        logger.info(f"범위 내 중복 제거 후 총 {len(items)}건")

        if items:
            year, month, week_num = week.split('-')
            key = f"newslatter/bronze/{self.source}/year={year}/month={month}/week={week_num}/news.json"
            self.storage_client.upload(key=key, value=items)
            logger.info(f"[DONE] {week} - {len(items)}건 업로드")

    @staticmethod
    def _in_date_range(pub_date: str, start: date, end: date) -> bool:
        try:
            dt = parsedate_to_datetime(pub_date)
            article_date = dt.date()
            return start <= article_date <= end
        except Exception:
            return True  # 날짜 파싱 실패 시 통과

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
