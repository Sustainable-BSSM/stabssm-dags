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
from src.infra.newslatter.greeting_generator import GreetingGenerator
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
        self._greeting = GreetingGenerator()
        self._tech_tip = TechTipGenerator()

    def __call__(self, week: str):
        asyncio.run(self._run(week))

    async def _run(self, week: str):
        school_df = self._school_reader.read_representatives(week)
        it_df = self._it_reader.read_representatives(week)

        if school_df.is_empty() and it_df.is_empty():
            logger.warning(f"콘텐츠 없음 (week={week}), 종료")
            return

        school_section, it_section, tech_tip, greeting = await asyncio.gather(
            self._rewriter.write_section(school_df.to_dicts(), "학교", week),
            self._rewriter.write_section(it_df.to_dicts(), "IT 업계", week),
            self._tech_tip.generate(it_df.to_dicts()),
            self._greeting.generate(week, date.today()),
        )

        year, month, _ = week.split("-")
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = self._render_pdf(week, school_section, it_section, tech_tip, greeting, tmpdir)
            upload_newsletter(pdf_path, year=year, month=month)
        logger.info(f"[GenerateNewsletterJob] 완료: week={week}")

    def _render_pdf(
        self,
        week: str,
        school_section: dict,
        it_section: dict,
        tech_tip: str,
        greeting: str,
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

        if greeting:
            doc.write(ArticleBlock(title="✉️ 아리의 인사말", body=greeting, styles=styles))
            doc.write(Divider())

        if school_section.get("sections"):
            doc.write(NoticeBlock("📰 학교 동향")).write(Divider())
            _write_sections(doc, school_section, styles)

        if it_section.get("sections"):
            doc.write(Divider())
            doc.write(NoticeBlock("💻 IT 업계 동향")).write(Divider())
            _write_sections(doc, it_section, styles)

        if tech_tip:
            doc.write(Divider())
            doc.write(NoticeBlock(f"💡 {tech_tip}"))

        doc.build()
        logger.info(f"[GenerateNewsletterJob] PDF 생성: {out_path}")
        return str(out_path)


_LINK_COLOR = "#0066cc"


def _write_sections(doc, section: dict, styles) -> None:
    sections = section["sections"]
    references = section.get("references", [])
    for i, s in enumerate(sections):
        if i > 0:
            doc.write(Divider())
        body = s["body"]
        if i == len(sections) - 1 and references:
            ref_links = "  ".join([
                f'<a href="{r["link"]}"><font color="{_LINK_COLOR}">{r["title"]}</font></a>'
                for r in references
            ])
            body = f'{body}<br/><br/>📎 참고 기사&nbsp;&nbsp;{ref_links}'
        doc.write(ArticleBlock(title=s["title"], body=body, styles=styles))


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
