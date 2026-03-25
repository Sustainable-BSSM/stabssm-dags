import argparse
import logging
from typing import Optional

import polars as pl

from src.core.jobs import Job
from src.core.repository.wanted.jobs_gold import WantedJobsGoldRepository
from src.core.repository.wanted.jobs_silver import WantedJobsSilverRepository
from src.dependencies.repository.wanted_jobs_gold import get_wanted_jobs_gold_repository
from src.dependencies.repository.wanted_jobs_silver import get_wanted_jobs_silver_repository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 티어 기준 (점수 합산, 최대 11점)
# ──────────────────────────────────────────────
# 정규직(regular)              : +3
# 신입 가능(is_newbie)          : +2
# 병역특례(alternative_mil)    : +3
# annual_to == 100 (경력 무관)  : +3
# annual_to >= 10  (경력 범위 넓음, 100 제외) : +1
#
# S : 11  A+: 9~10  A: 7~8  B: 4~6  C: 0~3
# ──────────────────────────────────────────────
#
# 학위 요구 키워드 (requirements 필드 검사)
# 매칭 시 requires_degree=True → 티어 C 강등
# ──────────────────────────────────────────────
DEGREE_KEYWORDS = [
    "학사", "석사", "박사",
    "4년제", "대졸", "대학교 졸업", "대학 졸업",
    "학위", "PhD", "MS 이상", "BS 이상",
]

def _requires_degree(requirements: Optional[str]) -> bool:
    if not requirements:
        return False
    return any(kw in requirements for kw in DEGREE_KEYWORDS)


def _score(row: dict) -> int:
    score = 0
    if row.get("employment_type") == "regular":
        score += 3
    if row.get("is_newbie"):
        score += 2
    if row.get("is_alternative_military"):
        score += 3
    annual_to = row.get("annual_to", 0) or 0
    if annual_to == 100:
        score += 3
    elif annual_to >= 10:
        score += 1
    return score


def _tier(score: int) -> str:
    if score >= 11:
        return "S"
    if score >= 9:
        return "A+"
    if score >= 7:
        return "A"
    if score >= 4:
        return "B"
    return "C"


class TierWantedJobsJob(Job):

    def __init__(self, silver_repo: WantedJobsSilverRepository, gold_repo: WantedJobsGoldRepository):
        self._silver_repo = silver_repo
        self._gold_repo = gold_repo

    def __call__(self, ds: str):
        df = self._silver_repo.read(ds)
        if df.is_empty():
            logger.info(f"silver 데이터 없음 (ds={ds}), 종료")
            return

        logger.info(f"silver 로드: {len(df)}건")
        df = self._transform(df, ds)

        degree_count = df["requires_degree"].sum()
        logger.info(f"학위 요구 공고: {degree_count}건 → 티어 C로 강등")
        all_tiers = pl.DataFrame({"tier": ["S", "A+", "A", "B", "C"]})
        tier_counts = (
            all_tiers
            .join(df.group_by("tier").len(), on="tier", how="left")
            .with_columns(pl.col("len").fill_null(0))
        )
        logger.info(f"티어 분류 완료:\n{tier_counts}")

        self._gold_repo.save(df, ds)

    def _transform(self, df: pl.DataFrame, ds: str) -> pl.DataFrame:
        # 병역특례 여부를 별도 boolean 필드로 분리
        df = df.with_columns(
            pl.col("additional_apply_type")
            .str.contains("alternative_military")
            .fill_null(False)
            .alias("is_alternative_military")
        )

        # 학위 요구 여부 판별
        rows = df.to_dicts()
        degree_flags = [
            _requires_degree(r.get("requirements")) or "전문연구요원" in (r.get("position") or "")
            for r in rows
        ]
        scores = [_score(r) for r in rows]
        tiers = ["C" if deg else _tier(s) for deg, s in zip(degree_flags, scores)]

        return (
            df
            .with_columns([
                pl.Series("requires_degree", degree_flags, dtype=pl.Boolean),
                pl.Series("score", scores, dtype=pl.Int8),
                pl.Series("tier", tiers, dtype=pl.String),
                pl.lit(ds).alias("dt"),
            ])
            .drop(["additional_apply_type", "requirements"])  # 가공된 필드(is_alternative_military, requires_degree)로 대체
        )


def run_job(ds: str):
    silver_repo = get_wanted_jobs_silver_repository()
    gold_repo = get_wanted_jobs_gold_repository()
    job = TierWantedJobsJob(silver_repo=silver_repo, gold_repo=gold_repo)
    job(ds=ds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, type=str)
    args = p.parse_args()
    run_job(ds=args.ds)
