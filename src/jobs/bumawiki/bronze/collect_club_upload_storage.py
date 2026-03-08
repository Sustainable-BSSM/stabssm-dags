import argparse

from src.common.const.year_established import FIRST_BSSM_YEAR
from src.common.util.bssm_generation_calculator import BSSMGenerationCalculator
from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki.club import BumawikiClubCrawler
from src.infra.requester.http import HttpRequester


class CollectClubJob(Job):

    def __init__(
            self,
            club_crawler: Crawler,
            storage_client: StorageClient
    ):
        self.club_crawler = club_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str):
        crawled_data = self.club_crawler.run()

        # 1기는 전공동아리를 두번 한 것으로 추정됨.
        # 1기 전공동아리 합병
        first_year = str(FIRST_BSSM_YEAR)
        second_year = str(FIRST_BSSM_YEAR + 1)
        if first_year in crawled_data:
            crawled_data.setdefault(second_year, []).extend(crawled_data.pop(first_year))

        for year in crawled_data:
            students = crawled_data[year]

            generation = BSSMGenerationCalculator.calculate(year=year) - 1
            self.storage_client.upload(
                key=f"bronze/bumawiki/club/dt={ds}/{generation}/club.json",
                value=students
            )


def run_job(ds: str):
    requester = HttpRequester()
    club_crawler = BumawikiClubCrawler(requester=requester)

    storage_client = get_storage_client()

    collect_club_job = CollectClubJob(
        club_crawler=club_crawler,
        storage_client=storage_client
    )

    collect_club_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
