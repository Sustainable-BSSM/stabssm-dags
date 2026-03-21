import logging
from urllib.parse import urlparse

from src.dependencies.llm import get_ari_llm

logger = logging.getLogger(__name__)

_LINK_COLOR = "#0066cc"

_PROMPT = """발간 기준: {issue}
아래는 해당 주차 {category} 관련 뉴스 원문들이야.

{articles}

위 뉴스들을 읽고, 실제로 다른 주제끼리만 섹션을 나눠서 작성해줘.

섹션 구분 기준:
- 같은 사건이나 인물을 다룬 기사들 → 하나의 섹션으로 통합
- 기사 수와 섹션 수가 같을 필요 없어
- 섹션은 최대 2~3개로 유지해

작성 규칙:
- 독자는 BSSM 학생, 교사, 학부모야
- 각 섹션은 2~3문장으로 작성해
- 단순 나열 말고, 맥락과 의미를 담아서
- 헤드라인은 해당 섹션 내용을 아우르는 한 줄로

반드시 아래 형식으로만 응답해 (다른 내용 없이, 섹션 사이 구분자 === 필수):
===
제목: [헤드라인]
본문: [2~3문장 내용]
==="""


class ArticleRewriter:

    def __init__(self):
        self._llm = get_ari_llm()

    async def write_section(self, articles: list[dict], category: str, week: str) -> dict:
        """여러 기사를 대주제별 섹션으로 나눠 작성하고 참고 링크 목록을 반환합니다."""
        if not articles:
            return {"sections": [], "references": []}

        year, month, week_num = week.split("-")
        issue = f"{year}년 {int(month)}월 {int(week_num)}주차"

        article_texts = "\n".join([
            f"[{i+1}] 제목: {a.get('title', '')}\n    내용: {a.get('description', '')}"
            for i, a in enumerate(articles)
        ])

        try:
            result = await self._llm.chat(
                query=_PROMPT,
                variables=[issue, category, article_texts],
            )
            sections = _parse(result)
        except Exception as e:
            logger.warning(f"[ArticleRewriter] LLM 실패, 원문 사용: {e}")
            sections = [{"title": f"{issue} {category} 소식",
                         "body": " ".join(a.get("description", "") for a in articles)}]

        references = [
            {"title": a.get("title", ""), "link": a.get("original_link", "")}
            for a in articles
            if _is_valid_url(a.get("original_link", ""))
        ]

        return {"sections": sections, "references": references}


def _parse(result: str) -> list[dict]:
    sections = []
    for block in result.split("==="):
        block = block.strip()
        if not block:
            continue
        title = ""
        body = ""
        for line in block.splitlines():
            if line.startswith("제목:"):
                title = line.removeprefix("제목:").strip()
            elif line.startswith("본문:"):
                body = line.removeprefix("본문:").strip()
        if title or body:
            sections.append({"title": title, "body": body})
    return sections


def _is_valid_url(url: str) -> bool:
    try:
        r = urlparse(url)
        return r.scheme in ("http", "https") and bool(r.netloc)
    except Exception:
        return False
