from src.core.client.storage import StorageClient
from src.dependencies.storage_client import get_storage_client
from src.jobs.newslatter.bronze.collect_news_upload_storage import CollectNaverNewsJob

IT_QUERIES = [
    "IT 업계",
    "소프트웨어 개발 트렌드",
    "테크 스타트업",
    "인공지능 개발자",
]


def run_job(week: str):
    storage_client: StorageClient = get_storage_client()
    job = CollectNaverNewsJob(storage_client=storage_client, queries=IT_QUERIES, source="it")
    job(week=week)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
