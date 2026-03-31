import argparse
import asyncio
import logging
import tempfile
from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
)

from src.core.jobs import Job
from core.pdf.components import ArticleBlock, Divider, SectionHeader
from core.pdf.fonts import FONT_NAME, register_fonts
from core.pdf.frame import BSSMNewsLatterFrame
from core.pdf.header_footer import NewsletterHeader
from core.pdf.styles import NewsletterStyleSheet

register_fonts()

_PAGE_WIDTH, _PAGE_HEIGHT = A4
_MARGIN = 72

from src.dependencies.repository.newslatter_it_gold_reader import get_it_gold_reader
from src.dependencies.repository.newslatter_school_gold_reader import (
    get_school_gold_reader,
)
from src.dependencies.repository.wanted_jobs_gold import get_wanted_jobs_gold_repository
from src.infra.newslatter.article_rewriter import ArticleRewriter
from src.infra.newslatter.discord_events import fetch_upcoming_events
from src.infra.newslatter.gdrive_uploader import upload_newsletter
from src.infra.newslatter.greeting_generator import GreetingGenerator
from src.infra.newslatter.job_postings_section import build_job_postings_section
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
        self._jobs_repo = get_wanted_jobs_gold_repository()
        self._rewriter = ArticleRewriter()
        self._greeting = GreetingGenerator()
        self._tech_tip = TechTipGenerator()

    def __call__(self, week: str):
        asyncio.run(self._run(week))

    async def _run(self, week: str):
        school_df = self._school_reader.read_representatives(week)
        it_df = self._it_reader.read_representatives(week)
        jobs_df = self._jobs_repo.read_top(ds=date.today().isoformat(), n=5)
        job_section = build_job_postings_section(jobs_df)

        if (
            school_df.is_empty()
            and it_df.is_empty()
            and not job_section.get("sections")
        ):
            logger.warning(f"콘텐츠 없음 (week={week}), 종료")
            return

        school_section, it_section, tech_tip, greeting = await asyncio.gather(
            self._rewriter.write_section(school_df.to_dicts(), "학교", week),
            self._rewriter.write_section(it_df.to_dicts(), "IT 업계", week),
            self._tech_tip.generate(it_df.to_dicts()),
            self._greeting.generate(week, date.today()),
        )

        events = fetch_upcoming_events(week)

        year, month, _ = week.split("-")
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = self._render_pdf(
                week,
                school_section,
                it_section,
                job_section,
                tech_tip,
                greeting,
                events,
                tmpdir,
            )
            upload_newsletter(pdf_path, year=year, month=month)
        logger.info(f"[GenerateNewsletterJob] 완료: week={week}")

    def _render_pdf(
        self,
        week: str,
        school_section: dict,
        it_section: dict,
        job_section: dict,
        tech_tip: str,
        greeting: str,
        events: list,
        output_dir: str,
    ) -> str:
        styles = NewsletterStyleSheet()
        out_path = Path(output_dir) / f"newsletter_{week}.pdf"

        logo_path = "/app/assets/bssm_logo.png"

        layout = BSSMNewsLatterFrame()
        content_frame = layout.frame

        # 첫 페이지: 로고 + 제목 + 라인
        first_frame = Frame(
            x1=_MARGIN,
            y1=_MARGIN,
            width=_PAGE_WIDTH - 2 * _MARGIN,
            height=_PAGE_HEIGHT - 2 * _MARGIN,
        )
        first_tpl = PageTemplate(
            id="FirstPage",
            frames=[first_frame],
            pagesize=A4,
            onPage=lambda c, d: _draw_first_page(c, d, logo_path),
        )

        # 2페이지 이후: 제목 + 라인 (로고 없음)
        normal_tpl = PageTemplate(
            id="Normal",
            frames=[content_frame],
            pagesize=A4,
            onPage=lambda c, d: _draw_normal_page(c, d),
        )

        # 마지막 페이지: end.png 전체
        end_frame = Frame(
            x1=_MARGIN,
            y1=_MARGIN,
            width=_PAGE_WIDTH - 2 * _MARGIN,
            height=_PAGE_HEIGHT - 2 * _MARGIN,
        )
        end_tpl = PageTemplate(
            id="EndPage",
            frames=[end_frame],
            pagesize=A4,
            onPage=lambda c, d: _draw_end_page(c, d, "/app/assets/end.png"),
        )

        doc = BaseDocTemplate(
            filename=str(out_path),
            pageTemplates=[first_tpl, normal_tpl, end_tpl],
        )

        story: list = []
        # 첫 페이지 이후 Normal 템플릿으로 전환
        story.append(NextPageTemplate("Normal"))

        if greeting:
            story.append(SectionHeader("인삿말"))
            story.append(ArticleBlock(title="", body=greeting, styles=styles))
            story.append(Divider())

        if school_section.get("sections"):
            story.append(SectionHeader("학교 동향", "이번 주 우리 학교 소식"))
            _append_sections(story, school_section, styles)
            story.append(Divider())

        story.append(
            SectionHeader("최근 프로젝트 홍보", "학생들의 프로젝트를 소개합니다")
        )
        story.append(Divider())

        if tech_tip:
            story.append(SectionHeader("꿀팁"))
            story.append(ArticleBlock(title="", body=tech_tip, styles=styles))
            story.append(Divider())

        if it_section.get("sections"):
            story.append(SectionHeader("IT 업계 동향", "이번 주 IT 업계 주요 소식"))
            _append_sections(story, it_section, styles)
            story.append(Divider())

        if job_section.get("sections"):
            story.append(SectionHeader("기회", "도전해 볼 만한 기회를 모았습니다"))
            _append_sections(story, job_section, styles)
            story.append(Divider())

        story.append(
            SectionHeader("다가오는 교내 이벤트", "놓치지 마세요, 다가오는 교내 일정")
        )
        if events:
            for ev in events:
                start = ev["start"][:10] if ev.get("start") else ""
                end = ev["end"][:10] if ev.get("end") else ""
                period = f"{start} ~ {end}" if end else start
                location = f" | {ev['location']}" if ev.get("location") else ""
                body = f"{period}{location}"
                if ev.get("description"):
                    body += f"<br/>{ev['description']}"
                story.append(ArticleBlock(title=ev["name"], body=body, styles=styles))
        story.append(Divider())

        story.append(NextPageTemplate("EndPage"))
        story.append(PageBreak())
        story.append(SectionHeader(""))

        doc.build(story)
        logger.info(f"[GenerateNewsletterJob] PDF 생성: {out_path}")
        return str(out_path)


_LINK_COLOR = "#0066cc"


def _draw_first_page(canvas, doc, logo_path: str) -> None:
    """첫 페이지 헤더: 로고(왼쪽) + BSSM NEWSLETTER(가운데) + 하단 라인."""
    canvas.saveState()

    # 로고
    from reportlab.lib.utils import ImageReader

    img = ImageReader(logo_path)
    iw, ih = img.getSize()
    logo_h = 12 * mm
    logo_w = iw * (logo_h / ih)
    canvas.drawImage(
        logo_path,
        _MARGIN,
        _PAGE_HEIGHT - _MARGIN + 2 * mm,
        width=logo_w,
        height=logo_h,
        mask="auto",
    )

    # 제목
    canvas.setFont(FONT_NAME, 18)
    canvas.setFillColor(colors.HexColor("#1a1a2e"))
    tw = canvas.stringWidth("BSSM NEWSLETTER", FONT_NAME, 18)
    canvas.drawString(
        (_PAGE_WIDTH - tw) / 2, _PAGE_HEIGHT - _MARGIN + 6 * mm, "BSSM NEWSLETTER"
    )

    # 하단 라인
    canvas.setStrokeColor(colors.HexColor("#cccccc"))
    canvas.setLineWidth(0.5)
    canvas.line(
        _MARGIN, _PAGE_HEIGHT - _MARGIN, _PAGE_WIDTH - _MARGIN, _PAGE_HEIGHT - _MARGIN
    )
    canvas.restoreState()


def _draw_normal_page(canvas, doc) -> None:
    pass  # 헤더 없음


def _draw_end_page(canvas, doc, end_image_path: str) -> None:
    canvas.saveState()
    canvas.drawImage(
        end_image_path,
        x=0,
        y=0,
        width=_PAGE_WIDTH,
        height=_PAGE_HEIGHT,
    )
    canvas.restoreState()


def _append_sections(story: list, section: dict, styles) -> None:
    sections = section["sections"]
    references = section.get("references", [])
    for i, s in enumerate(sections):
        if i > 0:
            story.append(Divider())
        body = s["body"]
        if i == len(sections) - 1 and references:
            ref_links = "  ".join(
                [
                    f'<a href="{r["link"]}"><font color="{_LINK_COLOR}">{r["title"]}</font></a>'
                    for r in references
                ]
            )
            body = f"{body}<br/><br/>참고 기사&nbsp;&nbsp;{ref_links}"
        story.append(ArticleBlock(title=s["title"], body=body, styles=styles))


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
