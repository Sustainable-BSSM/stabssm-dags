import asyncio
import logging

import polars as pl

from src.core.jobs import Job
from src.core.newslatter.scorer import NewsScorer as AbstractNewsScorer
from src.core.repository.newslatter.news_gold import NewsGoldRepository
from src.core.repository.newslatter.news_silver import NewsSilverRepository
from src.dependencies.repository.newslatter_news_gold import get_news_gold_repository
from src.dependencies.repository.newslatter_news_silver import get_news_silver_repository
from src.infra.newslatter.embedder import NewsEmbedder
from src.infra.newslatter.news_scorer import NewsScorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TOP_N_PER_WEEK = 5


class CurateNewsJob(Job):

    def __init__(
            self,
            silver_repo: NewsSilverRepository,
            gold_repo: NewsGoldRepository,
            scorer: AbstractNewsScorer = NewsScorer(),
    ):
        self._silver_repo = silver_repo
        self._gold_repo = gold_repo
        self._embedder = NewsEmbedder()
        self._scorer = scorer

    def __call__(self, week: str):
        asyncio.run(self._run(week))

    async def _run(self, week: str):
        df = self._silver_repo.read(week)
        if df.is_empty():
            logger.info("silver 데이터 없음, 종료")
            return

        logger.info(f"silver 로드: {len(df)}건")

        rows = df.to_dicts()

        # 1. 임베딩 + 의미적 중복 제거
        texts = [f"{r['title']} {r['description']}" for r in rows]
        embeddings = self._embedder.encode(texts)
        clusters = self._embedder.cluster(embeddings, threshold=0.85)

        representatives = [
            rows[max(cluster, key=lambda i: len(rows[i].get("description") or ""))]
            for cluster in clusters
        ]
        logger.info(f"중복 제거: {len(rows)}건 → {len(representatives)}건")

        # 2. LLM 점수 산정
        scores = await asyncio.gather(*[self._scorer.score(r) for r in representatives])

        # 3. 최종 점수 계산
        max_len = max(len(r.get("description") or "") for r in representatives) or 1
        result_rows = []
        for rep, score in zip(representatives, scores):
            body_length_score = len(rep.get("description") or "") / max_len
            llm_score = (score["relevance_score"] + score["publisher_score"]) / 2
            result_rows.append({
                **rep,
                "category": score["category"],
                "relevance_score": score["relevance_score"],
                "publisher_score": score["publisher_score"],
                "body_length_score": round(body_length_score, 4),
                "final_score": round(body_length_score * 0.3 + llm_score * 0.7, 4),
                "is_representative": False,
            })

        # 4. 주차별 상위 N개를 대표 기사로 선정
        result_df = pl.DataFrame(result_rows)
        result_df = self._mark_representatives(result_df)

        self._gold_repo.save(result_df)
        logger.info(f"gold 저장 완료: {len(result_df)}건")

    def _mark_representatives(self, df: pl.DataFrame) -> pl.DataFrame:
        top_links = (
            df.sort("final_score", descending=True)
            .group_by(["year", "month", "week"])
            .head(TOP_N_PER_WEEK)
            .select("link")
            .to_series()
            .to_list()
        )
        return df.with_columns(
            pl.col("link").is_in(top_links).alias("is_representative")
        )


def run_job(week: str):
    silver_repo = get_news_silver_repository()
    gold_repo = get_news_gold_repository()
    job = CurateNewsJob(silver_repo=silver_repo, gold_repo=gold_repo)
    job(week=week)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
