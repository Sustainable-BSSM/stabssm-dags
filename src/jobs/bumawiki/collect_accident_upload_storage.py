import argparse
from src.common.const.year_established import FIRST_BSSM_YEAR
from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki_student import BumawikiStudentCrawler
from src.infra.requester.http import HttpRequester

class CollectAccidentJob(Job):

    def __init__(
            self,
            accident_crawler: Crawler,
            storage_client: StorageClient
    ):
        self.accident_crawler = accident_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str):
        crawled_data = self.accident_crawler.run()
        for year in crawled_data:
            students = crawled_data[year]
            generation = FIRST_BSSM_YEAR - int(year) # 기수, ex) 1기, 2기 ···
            self.storage_client.upload(
                key=f"bronze/bumawiki/accident/dt={ds}/{generation}/accident.json",
                value=students
            )


def run_job(ds: str):
    requester = HttpRequester()
    accident_crawler = BumawikiStudentCrawler(requester=requester)

    storage_client = get_storage_client()

    collect_accident_job = CollectAccidentJob(
        accident_crawler=accident_crawler,
        storage_client=storage_client
    )

    collect_accident_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
