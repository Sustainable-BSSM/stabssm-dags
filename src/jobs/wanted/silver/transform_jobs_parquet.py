import argparse
import logging

import polars as pl

from src.core.jobs import Job
from src.core.repository.wanted.jobs import WantedJobsRepository
from src.core.repository.wanted.jobs_raw import WantedJobsRawRepository
from src.dependencies.repository.wanted_jobs import get_wanted_jobs_repository
from src.dependencies.repository.wanted_jobs_raw import get_wanted_jobs_raw_repository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 결측값 허용 불가 필드
_REQUIRED_STR_COLS = ["position", "company_name", "employment_type", "location"]


class TransformWantedJobsToParquetJob(Job):

    def __init__(self, raw_repo: WantedJobsRawRepository, jobs_repo: WantedJobsRepository):
        self._raw_repo = raw_repo
        self._jobs_repo = jobs_repo

    def __call__(self, ds: str):
        df = self._raw_repo.read(ds)
        if df.is_empty():
            logger.info(f"bronze 데이터 없음 (ds={ds}), 종료")
            return

        before = len(df)
        df = self._transform(df)
        logger.info(f"변환 완료: {before}건 → {len(df)}건")

        self._jobs_repo.save(df, ds)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        # 결측값 제거: 필수 문자열 컬럼이 null이거나 빈 문자열인 행 제거
        for col in _REQUIRED_STR_COLS:
            before = len(df)
            df = df.filter(pl.col(col).is_not_null() & (pl.col(col) != ""))
            removed = before - len(df)
            if removed:
                logger.info(f"결측값 제거 ({col}): {removed}건")

        # 중복 제거: job id 기준
        before = len(df)
        df = df.unique(subset=["id"], keep="first")
        removed = before - len(df)
        if removed:
            logger.info(f"중복 제거 (id): {removed}건")

        # additional_apply_type: list → 쉼표 구분 문자열로 정규화
        df = df.with_columns(
            pl.col("additional_apply_type")
            .map_elements(
                lambda v: ",".join(v) if v is not None else None,
                return_dtype=pl.String,
            )
            .alias("additional_apply_type")
        )

        return df


def run_job(ds: str):
    raw_repo = get_wanted_jobs_raw_repository()
    jobs_repo = get_wanted_jobs_repository()
    job = TransformWantedJobsToParquetJob(raw_repo=raw_repo, jobs_repo=jobs_repo)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()
    run_job(ds=args.ds)
