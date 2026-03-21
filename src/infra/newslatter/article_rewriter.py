import logging
import re
from urllib.parse import urlparse

from src.dependencies.llm import get_ari_llm

logger = logging.getLogger(__name__)

_LINK_COLOR = "#0066cc"

_PROMPT = """발간 기준: {issue}
아래는 해당 주차 {category} 관련 뉴스 원문들이야.

{articles}

[1단계] 그룹핑
기사들을 읽고, 같은 사건/인물/이슈를 다루는 기사 번호를 먼저 묶어줘.
같은 사건을 다른 언론사가 보도한 것도 같은 그룹이야.
예: 그룹1: [1,2,3] / 그룹2: [4,5]

[2단계] 섹션 작성
각 그룹에 대해 아래 형식으로 작성해줘.
- 독자는 BSSM 학생, 교사, 학부모야
- 본문은 4~6문장으로 작성해. 관찰 → 설명 → 의미 → 공감 순서로
- 단순 사실 나열 말고, 이 소식이 왜 중요한지 의미를 꼭 담아
- 공감이나 말걸기 문장 한 줄 포함해
- 헤드라인은 해당 그룹 내용을 아우르는 한 줄로

반드시 아래 형식으로만 응답해 (1단계 결과 먼저, 그 다음 섹션들, 섹션 사이 구분자 === 필수):
[그룹핑 결과]
그룹1: [번호들] / 그룹2: [번호들] ...

===
제목: [헤드라인]
본문: [4~6문장 내용]
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
            f"[{i+1}] 제목: {a.get('title', '')}\n    내용: {_clean(a.get('description', ''))}"
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
                         "body": " ".join(_clean(a.get("description", "")) for a in articles)}]

        references = [
            {"title": a.get("title", ""), "link": a.get("original_link", "")}
            for a in articles
            if _is_valid_url(a.get("original_link", ""))
        ]

        return {"sections": sections, "references": references}


def _clean(text: str) -> str:
    """description에 섞인 메타데이터 노이즈 제거."""
    text = re.sub(r'\b\w+=\w+\b', '', text)
    return text.strip()


def _parse(result: str) -> list[dict]:
    """=== 구분자로 섹션을 파싱. 본문이 여러 줄에 걸쳐 있어도 모두 수집."""
    sections = []
    for block in result.split("==="):
        block = block.strip()
        if not block or block.startswith("[그룹핑 결과]"):
            continue
        title = ""
        body_lines = []
        in_body = False
        for line in block.splitlines():
            if line.startswith("제목:"):
                title = line.removeprefix("제목:").strip()
                in_body = False
            elif line.startswith("본문:"):
                first = line.removeprefix("본문:").strip()
                if first:
                    body_lines.append(first)
                in_body = True
            elif in_body and line.strip():
                body_lines.append(line.strip())
        body = " ".join(body_lines)
        if title or body:
            sections.append({"title": title, "body": body})
    return sections


def _is_valid_url(url: str) -> bool:
    try:
        r = urlparse(url)
        return r.scheme in ("http", "https") and bool(r.netloc)
    except Exception:
        return False
