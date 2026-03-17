import argparse
import asyncio
import json
import logging

from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.dependencies.repository.bumawiki_docs_raw import get_bumawiki_docs_raw_repository
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki.docs import BumawikiDocsCrawler
from src.infra.requester.http import HttpRequester

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class CollectDocsJob(Job):

    def __init__(
            self,
            docs_crawler: Crawler,
            storage_client: StorageClient,
            repository: BumaWikiDocsRawRepository,
    ):
        self.docs_crawler = docs_crawler
        self.storage_client = storage_client
        self.repository = repository

    def __call__(self, ds: str, titles: list[str]):
        logger.info(f"총 {len(titles)}개 문서 수집 시작 (ds={ds})")
        asyncio.run(self._run(ds, titles))
        logger.info("모든 문서 수집 완료")

    async def _run(self, ds: str, titles: list[str]):
        await asyncio.gather(*[self._collect(ds, title) for title in titles])

    async def _collect(self, ds: str, title: str):
        if self.repository.exists(ds=ds, title=title):
            logger.info(f"[SKIP] {title}")
            return

        logger.info(f"[START] {title}")
        crawled_data = await asyncio.to_thread(self.docs_crawler.run, title)
        docs_id = crawled_data["id"]
        docs_title = crawled_data["title"]
        self.storage_client.upload(
            key=f"bumawiki/bronze/docs/ds={ds}/docs-{docs_id}-{docs_title}.json",
            value=[crawled_data]
        )
        logger.info(f"[DONE] {title}")


def run_job(ds: str, titles: list[str]):
    requester = HttpRequester()
    docs_crawler = BumawikiDocsCrawler(requester=requester)

    storage_client = get_storage_client()
    repository = get_bumawiki_docs_raw_repository()

    collect_docs_job = CollectDocsJob(
        docs_crawler=docs_crawler,
        storage_client=storage_client,
        repository=repository,
    )

    collect_docs_job(ds=ds, titles=titles)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    p.add_argument('--titles', required=True, type=str)  # JSON array string
    args = p.parse_args()

    run_job(ds=args.ds, titles=json.loads(args.titles))
