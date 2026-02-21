import argparse
from src.common.util.bssm_generation_calculator import BSSMGenerationCalculator
from src.core.client.storage import StorageClient, FakeStorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki_student import BumawikiStudentCrawler
from src.infra.requester.http import HttpRequester

class CollectStudentJob(Job):

    def __init__(
            self,
            student_crawler: Crawler,
            storage_client: StorageClient
    ):
        self.student_crawler = student_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str):
        crawled_data = self.student_crawler.run()
        for year in crawled_data:
            students = crawled_data[year]
            generation = BSSMGenerationCalculator.calculate(year=year)
            self.storage_client.upload(
                key=f"bronze/bumawiki/student/dt={ds}/{generation}/student.json",
                value=students
            )


def run_job(ds: str):
    requester = HttpRequester()
    student_crawler = BumawikiStudentCrawler(requester=requester)

    storage_client = get_storage_client()

    collect_student_job = CollectStudentJob(
        student_crawler=student_crawler,
        storage_client=storage_client
    )

    collect_student_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
