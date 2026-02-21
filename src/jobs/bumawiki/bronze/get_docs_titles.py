import argparse
from datetime import datetime
from src.common.const.year_established import FIRST_BSSM_YEAR
from src.common.enum.bumawiki.docs_type import BumaWikiDocsType
from src.common.util.bssm_generation_calculator import BSSMGenerationCalculator
from src.core.client.storage import StorageClient
from src.core.jobs import Job
from src.dependencies.storage_client import get_storage_client

class GetDocsTitlesJob(Job):

    def __init__(
            self,
            storage_client: StorageClient
    ):
        self.storage_client = storage_client

    def __call__(self, ds: str):
        titles = []
        for docs_type in BumaWikiDocsType:
            year = int(ds[:4])
            now_generation = BSSMGenerationCalculator.calculate(year=year)

            type = docs_type.value
            for generation in range(1, now_generation):
                key = f"bronze/bumawiki/{type}/dt={ds}/{generation}/{type}.json"
                docs_list = self.storage_client.get(key=key)
                if docs_list is None:
                    break

                docs_titles = [docs['title'] for docs in docs_list]
                titles.extend(docs_titles)
        return titles


def run_job(ds: str):
    storage_client = get_storage_client()

    get_docs_titles_job = GetDocsTitlesJob(
        storage_client=storage_client
    )

    get_docs_titles_job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--ds', required=True, type=str)
    args = p.parse_args()

    run_job(ds=args.ds)
