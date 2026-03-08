import argparse

from src.core.jobs import Job
from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.dependencies.repository.bumawiki_docs_raw import get_bumawiki_docs_raw_repository


class TransformDocsToBronzeParquetJob(Job):

    def __init__(self, repository: BumaWikiDocsRawRepository):
        self._repository = repository

    def __call__(self, ds: str):
        self._repository.save(ds)


def run_job(ds: str):
    repository = get_bumawiki_docs_raw_repository()
    job = TransformDocsToBronzeParquetJob(repository=repository)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
