import argparse

from src.core.client.storage import StorageClient
from src.core.crawler import Crawler
from src.core.jobs import Job
from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.dependencies.repository.bumawiki_docs_raw import get_bumawiki_docs_raw_repository
from src.dependencies.storage_client import get_storage_client
from src.infra.crawler.bumawiki.docs import BumawikiDocsCrawler
from src.infra.requester.http import HttpRequester


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

    def __call__(self, ds: str, title: str):
        if self.repository.exists(ds=ds, title=title):
            return

        crawled_data = self.docs_crawler.run(title)
        docs_id = crawled_data["id"]
        docs_title = crawled_data["title"]
        self.storage_client.upload(
            key=f"bronze/bumawiki/docs/dt={ds}/docs-{docs_id}-{docs_title}.json",
            value=[crawled_data]
        )


def run_job(ds: str, title: str):
    requester = HttpRequester()
    docs_crawler = BumawikiDocsCrawler(requester=requester)

    storage_client = get_storage_client()
    repository = get_bumawiki_docs_raw_repository()

    collect_docs_job = CollectDocsJob(
        docs_crawler=docs_crawler,
        storage_client=storage_client,
        repository=repository,
    )

    collect_docs_job(ds=ds, title=title)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    p.add_argument('--title', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds, title=args.title)
