import argparse
import asyncio
import json
import logging

from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.schoolwiki.doc_detail import SchoolwikiDocDetailCrawler
from src.infra.requester.http import HttpRequester

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class CollectSchoolwikiDocsJob(Job):

    def __init__(
            self,
            detail_crawler: Crawler,
            storage_client: StorageClient,
    ):
        self.detail_crawler = detail_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str, docs: list[dict]):
        logger.info(f"총 {len(docs)}개 문서 수집 시작 (ds={ds})")
        asyncio.run(self._run(ds, docs))
        logger.info("모든 문서 수집 완료")

    async def _run(self, ds: str, docs: list[dict]):
        await asyncio.gather(*[self._collect(ds, doc) for doc in docs])

    async def _collect(self, ds: str, doc_meta: dict):
        slug = doc_meta["slug"]
        doc_id = doc_meta["id"]
        title = doc_meta["title"]

        if self._exists(ds, doc_id):
            logger.info(f"[SKIP] {title} ({slug})")
            return

        logger.info(f"[START] {title} ({slug})")
        detail = await asyncio.to_thread(self.detail_crawler.run, slug)

        merged = {**doc_meta, **(detail or {})}
        self.storage_client.upload(
            key=f"bronze/bumawiki/docs/dt={ds}/docs-{doc_id}-{title}.json",
            value=[merged],
        )
        logger.info(f"[DONE] {title} ({slug})")

    def _exists(self, ds: str, doc_id: str) -> bool:
        try:
            from src.common.config.s3 import S3Config
            from src.infra.duckdb import create_conn
            conn = create_conn()
            result = conn.execute(f"""
                SELECT COUNT(*) FROM read_json_auto(
                    's3://{S3Config.BUCKET_NAME}/bronze/bumawiki/docs/dt={ds}/*.json'
                )
                WHERE id = '{doc_id}'
            """).fetchone()
            return result[0] > 0
        except Exception:
            return False


def run_job(ds: str, docs: list[dict]):
    requester = HttpRequester()
    detail_crawler = SchoolwikiDocDetailCrawler(requester=requester)
    storage_client = get_storage_client()

    job = CollectSchoolwikiDocsJob(
        detail_crawler=detail_crawler,
        storage_client=storage_client,
    )
    job(ds=ds, docs=docs)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    p.add_argument("--docs", required=True, type=str)  # JSON array of doc metadata dicts
    args = p.parse_args()

    run_job(ds=args.ds, docs=json.loads(args.docs))
