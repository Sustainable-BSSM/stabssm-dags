import argparse
import asyncio
import logging
from datetime import date
from pathlib import Path

import polars as pl

from src.core.jobs import Job
from core.pdf.components import ArticleBlock, Divider, NoticeBlock
from core.pdf.document import NewsletterDocument
from core.pdf.frame import BSSMNewsLatterFrame
from core.pdf.styles import NewsletterStyleSheet
from src.dependencies.repository.newslatter_it_gold_reader import get_it_gold_reader
from src.dependencies.repository.newslatter_school_gold_reader import get_school_gold_reader
from src.infra.newslatter.tech_tip_generator import TechTipGenerator
from src.infra.repository.newslatter.news_gold_reader import IcebergNewsGoldReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class GenerateNewsletterJob(Job):

    def __init__(
            self,
            school_gold_reader: IcebergNewsGoldReader,
            it_gold_reader: IcebergNewsGoldReader,
            output_dir: str,
    ):
        self._school_reader = school_gold_reader
        self._it_reader = it_gold_reader
        self._output_dir = output_dir
        self._tech_tip = TechTipGenerator()

    def __call__(self, week: str):
        asyncio.run(self._run(week))

    async def _run(self, week: str):
        school_df = self._school_reader.read_representatives(week)
        it_df = self._it_reader.read_representatives(week)

        if it_df.is_empty():
            logger.warning(f"IT 뉴스 없음 (week={week}), 종료")
            return

        tech_tip = await self._tech_tip.generate(it_df.to_dicts())
        story = self._build_story(school_df, it_df, tech_tip)
        self._render_pdf(week, story)
        logger.info(f"[GenerateNewsletterJob] 완료: week={week}")

    def _build_story(self, school_df: pl.DataFrame, it_df: pl.DataFrame, tech_tip: str) -> list:
        styles = NewsletterStyleSheet()
        story = []

        if not school_df.is_empty():
            story.append(NoticeBlock("📰 학교 동향"))
            story.append(Divider())
            for row in school_df.to_dicts():
                story.append(ArticleBlock(
                    title=row.get("title", ""),
                    body=row.get("description", ""),
                    styles=styles,
                ))
                story.append(Divider())

        story.append(NoticeBlock("💻 IT 업계 동향"))
        story.append(Divider())
        for row in it_df.to_dicts():
            story.append(ArticleBlock(
                title=row.get("title", ""),
                body=row.get("description", ""),
                styles=styles,
            ))
            story.append(Divider())

        story.append(NoticeBlock(f"💡 {tech_tip}"))
        return story

    def _render_pdf(self, week: str, story: list) -> None:
        year, month, week_num = week.split("-")
        issue = f"{year}년 {int(month)}월 {int(week_num)}주차"
        layout = BSSMNewsLatterFrame()

        out_path = Path(self._output_dir) / f"newsletter_{week}.pdf"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        doc = NewsletterDocument(
            filename=str(out_path),
            layout=layout,
            title="BSSM 뉴스레터",
            issue=issue,
            date=date.today().strftime("%Y.%m.%d"),
        )
        doc.build(story)
        logger.info(f"[GenerateNewsletterJob] PDF 저장: {out_path}")


def run_job(week: str, output_dir: str):
    job = GenerateNewsletterJob(
        school_gold_reader=get_school_gold_reader(),
        it_gold_reader=get_it_gold_reader(),
        output_dir=output_dir,
    )
    job(week=week)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    p.add_argument("--output-dir", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week, output_dir=args.output_dir)
