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
    Flowable,
    Frame,
    HRFlowable,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
)
from reportlab.lib.styles import ParagraphStyle

from src.core.jobs import Job
from core.pdf.fonts import FONT_NAME, register_fonts
from core.pdf.frame import BSSMNewsLatterFrame

register_fonts()

# ---------------------------------------------------------------------------
# Design tokens
# ---------------------------------------------------------------------------
_PAGE_WIDTH, _PAGE_HEIGHT = A4
_MARGIN = 72

_PRIMARY = "#991B1B"  # 어두운 빨간색
_PRIMARY_LIGHT = "#FEF2F2"  # 연한 빨간 배경
_TEXT_DARK = "#111827"  # 거의 검정
_TEXT_BODY = "#374151"  # 본문 회색
_TEXT_MUTED = "#9CA3AF"  # 연한 회색 (서브타이틀)
_DIVIDER_COLOR = "#E5E7EB"  # 구분선
_LINK_COLOR = "#991B1B"  # 링크도 primary

# ---------------------------------------------------------------------------
# Custom styles (외부 패키지 스타일 대신 직접 정의)
# ---------------------------------------------------------------------------
_base_font = FONT_NAME

_style_section_title = ParagraphStyle(
    "DSectionTitle",
    fontName=_base_font,
    fontSize=16,
    leading=22,
    textColor=colors.HexColor(_PRIMARY),
    spaceBefore=2 * mm,
    spaceAfter=1 * mm,
)

_style_section_subtitle = ParagraphStyle(
    "DSectionSubtitle",
    fontName=_base_font,
    fontSize=9,
    leading=13,
    textColor=colors.HexColor(_TEXT_MUTED),
    spaceAfter=4 * mm,
)

_style_article_title = ParagraphStyle(
    "DArticleTitle",
    fontName=_base_font,
    fontSize=12,
    leading=16,
    textColor=colors.HexColor(_TEXT_DARK),
    spaceBefore=3 * mm,
    spaceAfter=1.5 * mm,
)

_style_body = ParagraphStyle(
    "DBody",
    fontName=_base_font,
    fontSize=10,
    leading=15,
    textColor=colors.HexColor(_TEXT_BODY),
    spaceAfter=2 * mm,
)


# ---------------------------------------------------------------------------
# Custom flowables
# ---------------------------------------------------------------------------
class _SectionHeader(Flowable):
    """섹션 헤더: 왼쪽 빨간 액센트 바 + 볼드 제목 + 서브타이틀."""

    def __init__(self, title: str, subtitle: str = ""):
        super().__init__()
        self._title = Paragraph(title, _style_section_title) if title else None
        self._subtitle = (
            Paragraph(subtitle, _style_section_subtitle) if subtitle else None
        )
        self._bar_width = 3
        self._bar_pad = 8

    def wrap(self, aW, aH):
        inner_w = aW - self._bar_pad - self._bar_width
        h = 0
        if self._title:
            self._title.wrap(inner_w, aH)
            h += self._title.height
        if self._subtitle:
            self._subtitle.wrap(inner_w, aH)
            h += self._subtitle.height
        self._width = aW
        self._height = h + 2 * mm
        return self._width, self._height

    def draw(self):
        # 왼쪽 빨간 액센트 바
        self.canv.setFillColor(colors.HexColor(_PRIMARY))
        self.canv.rect(0, 0, self._bar_width, self._height, fill=1, stroke=0)

        x = self._bar_width + self._bar_pad
        y = self._height
        if self._title:
            y -= self._title.height
            self._title.drawOn(self.canv, x, y)
        if self._subtitle:
            y -= self._subtitle.height
            self._subtitle.drawOn(self.canv, x, y)


class _ArticleBlock(Flowable):
    """기사 블록: 제목 + 본문."""

    def __init__(self, title: str, body: str):
        super().__init__()
        self._title = Paragraph(title, _style_article_title) if title else None
        self._body = Paragraph(body, _style_body) if body else None

    def wrap(self, aW, aH):
        h = 0
        if self._title:
            self._title.wrap(aW, aH)
            h += self._title.height
        if self._body:
            self._body.wrap(aW, aH)
            h += self._body.height
        self._width = aW
        self._height = h
        return self._width, self._height

    def draw(self):
        y = self._height
        if self._title:
            y -= self._title.height
            self._title.drawOn(self.canv, 0, y)
        if self._body:
            self._body.drawOn(self.canv, 0, 0)


def _divider():
    return HRFlowable(
        width="100%",
        thickness=0.5,
        color=colors.HexColor(_DIVIDER_COLOR),
        spaceAfter=5 * mm,
        spaceBefore=5 * mm,
    )


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from src.dependencies.repository.newslatter_it_gold_reader import get_it_gold_reader
from src.dependencies.repository.newslatter_school_gold_reader import (
    get_school_gold_reader,
)
from src.dependencies.repository.wanted_jobs_gold import get_wanted_jobs_gold_repository
from src.infra.newslatter.article_rewriter import ArticleRewriter
from src.infra.newslatter.discord_events import fetch_upcoming_events
from src.infra.newslatter.discord_forum import fetch_recent_projects
from src.infra.newslatter.gdrive_uploader import upload_newsletter
from src.infra.newslatter.greeting_generator import GreetingGenerator
from src.infra.newslatter.job_postings_section import build_job_postings_section
from src.infra.newslatter.tech_tip_generator import TechTipGenerator
from src.infra.repository.newslatter.news_gold_reader import IcebergNewsGoldReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------
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
        projects = fetch_recent_projects(week)

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
                projects,
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
        projects: list,
        output_dir: str,
    ) -> str:
        out_path = Path(output_dir) / f"newsletter_{week}.pdf"
        logo_path = "/app/assets/bssm_logo.png"

        layout = BSSMNewsLatterFrame()

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

        normal_frame = Frame(
            x1=_MARGIN,
            y1=_MARGIN,
            width=_PAGE_WIDTH - 2 * _MARGIN,
            height=_PAGE_HEIGHT - 2 * _MARGIN,
        )
        normal_tpl = PageTemplate(
            id="Normal",
            frames=[normal_frame],
            pagesize=A4,
            onPage=_draw_normal_page,
        )

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
        story.append(NextPageTemplate("Normal"))

        # 1. 인삿말
        if greeting:
            story.append(_SectionHeader("인삿말"))
            story.append(_ArticleBlock("", greeting))
            story.append(_divider())

        # 2. 학교 동향
        if school_section.get("sections"):
            story.append(_SectionHeader("학교 동향", "이번 주 우리 학교 소식"))
            _append_sections(story, school_section)
            story.append(_divider())

        # 3. 최근 프로젝트 홍보
        if projects:
            story.append(
                _SectionHeader("최근 프로젝트 홍보", "학생들의 프로젝트를 소개합니다")
            )
            for proj in projects:
                story.append(_ArticleBlock(proj["name"], proj.get("description", "")))
            story.append(_divider())

        # 4. 꿀팁
        if tech_tip:
            story.append(_SectionHeader("꿀팁"))
            story.append(_ArticleBlock("", tech_tip))
            story.append(_divider())

        # 5. IT 업계 동향
        if it_section.get("sections"):
            story.append(_SectionHeader("IT 업계 동향", "이번 주 IT 업계 주요 소식"))
            _append_sections(story, it_section)
            story.append(_divider())

        # 6. 기회
        if job_section.get("sections"):
            story.append(_SectionHeader("기회", "도전해 볼 만한 기회를 모았습니다"))
            _append_sections(story, job_section)
            story.append(_divider())

        # 7. 다가오는 교내 이벤트
        story.append(
            _SectionHeader("다가오는 교내 이벤트", "놓치지 마세요, 다가오는 교내 일정")
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
                story.append(_ArticleBlock(ev["name"], body))
        story.append(_divider())

        # 8. 마지막 페이지
        story.append(NextPageTemplate("EndPage"))
        story.append(PageBreak())
        story.append(Spacer(1, 1))

        doc.build(story)
        logger.info(f"[GenerateNewsletterJob] PDF 생성: {out_path}")
        return str(out_path)


# ---------------------------------------------------------------------------
# Page draw callbacks
# ---------------------------------------------------------------------------
def _draw_first_page(canvas, doc, logo_path: str) -> None:
    """첫 페이지: 로고(왼쪽) + BSSM NEWSLETTER(가운데) + 빨간 라인."""
    canvas.saveState()

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

    canvas.setFont(FONT_NAME, 18)
    canvas.setFillColor(colors.HexColor(_PRIMARY))
    title = "BSSM NEWSLETTER"
    tw = canvas.stringWidth(title, FONT_NAME, 18)
    canvas.drawString((_PAGE_WIDTH - tw) / 2, _PAGE_HEIGHT - _MARGIN + 6 * mm, title)

    # 어두운 빨간색 헤더 라인
    canvas.setStrokeColor(colors.HexColor(_PRIMARY))
    canvas.setLineWidth(1.5)
    canvas.line(
        _MARGIN, _PAGE_HEIGHT - _MARGIN, _PAGE_WIDTH - _MARGIN, _PAGE_HEIGHT - _MARGIN
    )

    canvas.restoreState()


def _draw_normal_page(canvas, doc) -> None:
    """2페이지 이후: 얇은 상단 빨간 라인만."""
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor(_PRIMARY))
    canvas.setLineWidth(0.5)
    canvas.line(
        _MARGIN,
        _PAGE_HEIGHT - _MARGIN + 2 * mm,
        _PAGE_WIDTH - _MARGIN,
        _PAGE_HEIGHT - _MARGIN + 2 * mm,
    )
    canvas.restoreState()


def _draw_end_page(canvas, doc, end_image_path: str) -> None:
    canvas.saveState()
    canvas.drawImage(end_image_path, x=0, y=0, width=_PAGE_WIDTH, height=_PAGE_HEIGHT)
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _append_sections(story: list, section: dict) -> None:
    sections = section["sections"]
    references = section.get("references", [])
    for i, s in enumerate(sections):
        if i > 0:
            story.append(Spacer(1, 2 * mm))
        body = s["body"]
        if i == len(sections) - 1 and references:
            ref_links = "  ".join(
                [
                    f'<a href="{r["link"]}"><font color="{_LINK_COLOR}">{r["title"]}</font></a>'
                    for r in references
                ]
            )
            body = f'{body}<br/><br/><font color="{_TEXT_MUTED}">참고 기사</font>&nbsp;&nbsp;{ref_links}'
        story.append(_ArticleBlock(s["title"], body))


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
