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

        # 1. 임베딩 + 의미적 중복 제거 — 클러스터 내 제목 관련성 기준으로 대표 선정
        texts = [f"{r['title']} {r['description']}" for r in rows]
        embeddings = self._embedder.encode(texts)
        clusters = self._embedder.cluster(embeddings, threshold=0.85)

        representatives = [
            rows[self._pick_representative(cluster, rows)]
            for cluster in clusters
        ]
        logger.info(f"중복 제거: {len(rows)}건 → {len(representatives)}건")

        # 2. LLM 점수 산정
        scores = await asyncio.gather(*[self._scorer.score(r) for r in representatives])

        # 3. 점수 부착 + relevance 낮은 기사 제거
        result_rows = []
        for rep, score in zip(representatives, scores):
            llm_score = score["relevance_score"] * 0.8 + score["publisher_score"] * 0.2
            # 제목에 수집 쿼리 키워드가 있으면 본교 직접 관련 기사로 간주하여 보너스
            query = rep.get("query", "").replace(" ", "")
            title = rep.get("title", "").replace(" ", "")
            title_bonus = 0.15 if (query and query in title) else 0.0
            result_rows.append({
                **rep,
                "category": score["category"],
                "relevance_score": score["relevance_score"],
                "publisher_score": score["publisher_score"],
                "final_score": round(llm_score + title_bonus, 4),
                "is_representative": False,
            })

        result_rows = [r for r in result_rows if r["relevance_score"] >= 0.6]
        logger.info(f"relevance 필터 후: {len(result_rows)}건")

        if not result_rows:
            logger.info("관련 기사 없음, 종료")
            return

        # 4. 주차별 상위 N개를 대표 기사로 선정
        result_df = pl.DataFrame(result_rows)
        result_df = self._mark_representatives(result_df)

        self._gold_repo.save(result_df)
        logger.info(f"gold 저장 완료: {len(result_df)}건")

    def _pick_representative(self, cluster: list[int], rows: list[dict]) -> int:
        """클러스터 내에서 제목에 수집 쿼리 키워드가 포함된 기사를 우선 선택.
        없으면 description이 가장 긴 기사 선택."""
        def title_has_query(i: int) -> bool:
            query = rows[i].get("query", "").replace(" ", "")
            title = rows[i].get("title", "").replace(" ", "")
            return bool(query and query in title)

        for i in cluster:
            if title_has_query(i):
                return i
        return max(cluster, key=lambda i: len(rows[i].get("description") or ""))

    def _mark_representatives(self, df: pl.DataFrame) -> pl.DataFrame:
        top_links: set[str] = set()
        counts: dict[tuple, int] = {}
        for row in df.sort("final_score", descending=True).iter_rows(named=True):
            key = (row["year"], row["month"], row["week"])
            counts[key] = counts.get(key, 0) + 1
            if counts[key] <= TOP_N_PER_WEEK:
                top_links.add(row["link"])
        return df.with_columns(
            pl.col("link").is_in(list(top_links)).alias("is_representative")
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
