import argparse
import logging

from src.dependencies.repository.newslatter_it_news_gold import get_it_news_gold_repository
from src.dependencies.repository.newslatter_it_news_silver import get_it_news_silver_repository
from src.jobs.newslatter.gold.curate_news import CurateNewsJob

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_job(week: str):
    silver_repo = get_it_news_silver_repository()
    gold_repo = get_it_news_gold_repository()
    job = CurateNewsJob(silver_repo=silver_repo, gold_repo=gold_repo)
    job(week=week)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
