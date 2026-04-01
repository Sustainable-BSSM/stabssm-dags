import argparse
import logging

from src.core.client.storage import StorageClient
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.wanted.jobs import WantedJobsCrawler
from src.infra.requester.http import HttpRequester

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class CollectWantedJobsJob(Job):

    def __init__(self, storage_client: StorageClient):
        self.storage_client = storage_client

    def __call__(self, ds: str):
        logger.info(f"원티드 채용공고 수집 시작 (ds={ds})")

        crawler = WantedJobsCrawler(requester=HttpRequester())
        postings = crawler.run()

        if not postings:
            logger.warning("수집된 채용공고가 없습니다.")
            return

        key = f"wanted/bronze/jobs/dt={ds}/jobs.json"
        self.storage_client.upload(key=key, value=[p.to_dict() for p in postings])
        logger.info(f"[DONE] {ds} - {len(postings)}건 업로드 → {key}")


def run_job(ds: str):
    storage_client = get_storage_client()
    job = CollectWantedJobsJob(storage_client=storage_client)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
