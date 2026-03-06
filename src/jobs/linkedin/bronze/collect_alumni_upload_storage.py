

import argparse
from dataclasses import asdict
from typing import List

from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.core.model.linkedin import LinkedInPerson
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki.docs import BumawikiDocsCrawler
from src.infra.crawler.linkedin.alumni import LinkedInAlumniCrawler
from src.infra.requester.http import HttpRequester
from src.infra.requester.playwright import PlaywrightRequester


class CollectBSSMAlumniJob(Job):

    def __init__(
            self,
            alumin_crawler: Crawler,
            storage_client: StorageClient
    ):
        self.alumin_crawler = alumin_crawler
        self.storage_client = storage_client

    def __call__(self, ds: str):
        alumnis : List[LinkedInPerson] = self.alumin_crawler.run()

        self.storage_client.upload(
            key=f"bronze/linkedin/alumni/dt={ds}/alumni.json",
            value=asdict(*alumnis)
        )


def run_job(ds: str):
    requester = HttpRequester()
    storage_client = get_storage_client()

    with PlaywrightRequester(headless=True) as playwright_requester:
        alumin_crawler = LinkedInAlumniCrawler(
            playwright_requester=playwright_requester,
            requester=requester
        )


        collect_docs_job = CollectBSSMAlumniJob(
            alumin_crawler=alumin_crawler,
            storage_client=storage_client
        )

        collect_docs_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
