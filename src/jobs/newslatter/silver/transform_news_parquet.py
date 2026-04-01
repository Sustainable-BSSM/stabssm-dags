import argparse
import logging
from email.utils import parsedate_to_datetime

import polars as pl

from src.core.jobs import Job
from src.core.repository.newslatter.news import NewsRepository
from src.core.repository.newslatter.news_raw import NewsRawRepository
from src.dependencies.repository.newslatter_news import get_news_repository
from src.dependencies.repository.newslatter_news_raw import get_news_raw_repository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _parse_pub_date(value: str | None):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        return None


class TransformNewsToParquetJob(Job):

    def __init__(self, raw_repo: NewsRawRepository, news_repo: NewsRepository):
        self._raw_repo = raw_repo
        self._news_repo = news_repo

    def __call__(self, week: str):
        df = self._raw_repo.read(week)
        if df.is_empty():
            logger.info(f"bronze 데이터 없음 (week={week}), 종료")
            return

        before = len(df)
        df = self._transform(df)
        logger.info(f"변환 완료: {before}건 → {len(df)}건 (누락 URL 제거 후)")

        self._news_repo.save(df, week)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.filter(
            pl.col("link").is_not_null() & (pl.col("link") != "") &
            pl.col("original_link").is_not_null() & (pl.col("original_link") != "")
        )

        df = df.with_columns(
            pl.col("pub_date")
            .map_elements(_parse_pub_date, return_dtype=pl.Datetime("us", "UTC"))
            .alias("pub_date")
        )

        return df


def run_job(week: str):
    raw_repo = get_news_raw_repository()
    news_repo = get_news_repository()
    job = TransformNewsToParquetJob(raw_repo=raw_repo, news_repo=news_repo)
    job(week=week)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
