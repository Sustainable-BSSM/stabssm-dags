import argparse
from src.common.const.year_established import FIRST_BSSM_YEAR
from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki_club import BumawikiClubCrawler
from src.infra.crawler.bumawiki_docs import BumawikiDocsCrawler
from src.infra.requester.http import HttpRequester

class CollectDocsJob(Job):

    def __init__(
            self,
            docs_crawler: Crawler,
            storage_client: StorageClient
    ):
        self.docs_crawler = docs_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str):
        crawled_data = self.docs_crawler.run()
        for year in crawled_data:
            students = crawled_data[year]
            generation = FIRST_BSSM_YEAR - int(year) # 기수, ex) 1기, 2기 ···
            self.storage_client.upload(
                key=f"bronze/bumawiki/club/dt={ds}/{generation}/club.json",
                value=students
            )


def run_job(ds: str):
    requester = HttpRequester()
    docs_crawler = BumawikiDocsCrawler(requester=requester)

    storage_client = get_storage_client()

    collect_docs_job = CollectDocsJob(
        docs_crawler=docs_crawler,
        storage_client=storage_client
    )

    collect_docs_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
