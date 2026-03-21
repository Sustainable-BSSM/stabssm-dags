import argparse
import asyncio
import logging
import tempfile
from datetime import date
from pathlib import Path

from src.core.jobs import Job
from core.pdf.components import ArticleBlock, Divider, NoticeBlock
from core.pdf.document import NewsletterDocument
from core.pdf.frame import BSSMNewsLatterFrame
from core.pdf.styles import NewsletterStyleSheet
from src.dependencies.repository.newslatter_it_gold_reader import get_it_gold_reader
from src.dependencies.repository.newslatter_school_gold_reader import get_school_gold_reader
from src.infra.newslatter.article_rewriter import ArticleRewriter
from src.infra.newslatter.gdrive_uploader import upload_newsletter
from src.infra.newslatter.tech_tip_generator import TechTipGenerator
from src.infra.repository.newslatter.news_gold_reader import IcebergNewsGoldReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class GenerateNewsletterJob(Job):

    def __init__(
            self,
            school_gold_reader: IcebergNewsGoldReader,
            it_gold_reader: IcebergNewsGoldReader,
    ):
        self._school_reader = school_gold_reader
        self._it_reader = it_gold_reader
        self._rewriter = ArticleRewriter()
        self._tech_tip = TechTipGenerator()

    def __call__(self, week: str):
        asyncio.run(self._run(week))

    async def _run(self, week: str):
        school_df = self._school_reader.read_representatives(week)
        it_df = self._it_reader.read_representatives(week)

        if school_df.is_empty() and it_df.is_empty():
            logger.warning(f"콘텐츠 없음 (week={week}), 종료")
            return

        school_articles, it_articles, tech_tip = await asyncio.gather(
            self._rewriter.rewrite_all(school_df.to_dicts()),
            self._rewriter.rewrite_all(it_df.to_dicts()),
            self._tech_tip.generate(it_df.to_dicts()),
        )

        year, month, _ = week.split("-")
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = self._render_pdf(week, school_articles, it_articles, tech_tip, tmpdir)
            upload_newsletter(pdf_path, year=year, month=month)
        logger.info(f"[GenerateNewsletterJob] 완료: week={week}")

    def _render_pdf(
        self,
        week: str,
        school_articles: list[dict],
        it_articles: list[dict],
        tech_tip: str,
        output_dir: str,
    ) -> str:
        styles = NewsletterStyleSheet()
        year, month, week_num = week.split("-")
        issue = f"{year}년 {int(month)}월 {int(week_num)}주차"
        out_path = Path(output_dir) / f"newsletter_{week}.pdf"

        doc = NewsletterDocument(
            filename=str(out_path),
            layout=BSSMNewsLatterFrame(),
            title="BSSM 뉴스레터",
            issue=issue,
            date=date.today().strftime("%Y.%m.%d"),
        )

        if school_articles:
            (
                doc
                    .write(NoticeBlock("📰 학교 동향"))
                    .write(Divider())
                    .write_each(
                        [ArticleBlock(title=a["title"], body=a["body"], styles=styles) for a in school_articles],
                        sep=Divider(),
                    )
            )

        if it_articles:
            (
                doc
                    .write(NoticeBlock("💻 IT 업계 동향"))
                    .write(Divider())
                    .write_each(
                        [ArticleBlock(title=a["title"], body=a["body"], styles=styles) for a in it_articles],
                        sep=Divider(),
                    )
            )

        if tech_tip:
            doc.write(NoticeBlock(f"💡 {tech_tip}"))

        doc.build()
        logger.info(f"[GenerateNewsletterJob] PDF 생성: {out_path}")
        return str(out_path)


def run_job(week: str):
    job = GenerateNewsletterJob(
        school_gold_reader=get_school_gold_reader(),
        it_gold_reader=get_it_gold_reader(),
    )
    job(week=week)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--week", required=True, type=str)
    args = p.parse_args()
    run_job(week=args.week)
