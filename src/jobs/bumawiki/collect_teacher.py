import argparse
from src.common.const.year_established import FIRST_BSSM_YEAR
from src.core.client.storage import StorageClient, FakeStorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki_teacher import BumawikiTeacherCrawler
from src.infra.requester.http import HttpRequester

class CollectTeacherJob(Job):

    def __init__(
            self,
            teacher_crawler: Crawler,
            storage_client: StorageClient
    ):
        self.teacher_crawler = teacher_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str):
        crawled_data = self.teacher_crawler.run()
        for year in crawled_data:
            teachers = crawled_data[year]
            generation = FIRST_BSSM_YEAR - int(year) # 기수, ex) 1기, 2기 ···
            self.storage_client.upload(
                key=f"bronze/bumawiki/teacher/dt={ds}/{generation}/teacher.json",
                value=teachers
            )


def run_job(ds: str):
    requester = HttpRequester()
    teacher_crawler = BumawikiTeacherCrawler(requester=requester)

    storage_client = get_storage_client()

    collect_teacher_job = CollectTeacherJob(
        teacher_crawler=teacher_crawler,
        storage_client=storage_client
    )

    collect_teacher_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
