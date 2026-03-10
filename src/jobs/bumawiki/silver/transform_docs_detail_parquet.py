import argparse

from src.core.jobs import Job
from src.core.repository.bumawiki.docs import BumaWikiDocsRepository
from src.core.repository.bumawiki.docs_raw import BumaWikiDocsRawRepository
from src.dependencies.repository.bumawiki_docs import get_bumawiki_docs_repository
from src.dependencies.repository.bumawiki_docs_raw import get_bumawiki_docs_raw_repository


class TransformDocsDetailToParquetJob(Job):

    def __init__(
            self,
            raw_repo: BumaWikiDocsRawRepository,
            docs_repo: BumaWikiDocsRepository,
    ):
        self._raw_repo = raw_repo
        self._docs_repo = docs_repo

    def __call__(self, ds: str):
        df = self._raw_repo.read(ds)
        if df.is_empty():
            return
        self._docs_repo.save(df, ds)


def run_job(ds: str):
    raw_repo = get_bumawiki_docs_raw_repository()
    docs_repo = get_bumawiki_docs_repository()
    job = TransformDocsDetailToParquetJob(raw_repo=raw_repo, docs_repo=docs_repo)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
