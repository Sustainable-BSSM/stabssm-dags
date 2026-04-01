import argparse
import logging

from src.dependencies.repository.newslatter_it_news import get_it_news_repository
from src.dependencies.repository.newslatter_it_news_raw import get_it_news_raw_repository
from src.jobs.newslatter.silver.transform_news_parquet import TransformNewsToParquetJob

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_job(week: str):
    raw_repo = get_it_news_raw_repository()
    news_repo = get_it_news_repository()
    job = TransformNewsToParquetJob(raw_repo=raw_repo, news_repo=news_repo)
    job(week=week)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
